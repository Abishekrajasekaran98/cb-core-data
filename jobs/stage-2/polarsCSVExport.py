"""
polars_csv_export.py — standalone job, NO Spark/findspark imports.

Each report's Spark job writes a PLAIN (non-partitioned) parquet file and
then stops. This script runs AFTER that job has fully exited, reads the
plain parquet, and does the per-org split + CSV write using Polars.

CHANGES vs the previous version (all marked "CHANGED" below):
  1. MEMORY: the old code did pl.read_parquet() of the whole file and then
     df.partition_by(), which copies every row into per-org frames while the
     full frame is still alive -> peak memory ~2x the dataset. At 220M rows
     (ACBP enrolment) the Linux OOM killer sent SIGKILL (exit -9, no stderr).
     Now the file is processed in hash-bucketed chunks of at most
     MAX_ROWS_PER_CHUNK rows. Each org lives in exactly one chunk, so output
     is identical, but peak memory is bounded by the chunk size.
  2. RERUN SAFETY: the old code wiped EVERY report's output dir up front and
     deleted each report's input parquet right after it succeeded. A rerun
     after a crash therefore destroyed finished output AND the input needed
     to rebuild it. Now an output dir is only wiped immediately before it is
     rewritten (and never when its input is missing), and input parquet is
     only deleted after the whole run succeeds.
  3. INTEGRITY: rows written are checked against rows in the source.
  4. LOGGING: line-buffered output + peak memory per report, so the last log
     line before a kill is trustworthy and memory growth is visible.
  5. EXIT CODE: a failing report no longer stops the others; the job still
     exits non-zero at the end so Airflow marks it failed.

HOW TO ADD A NEW REPORT:
  1. In that report's Spark job, replace its CSV-writing step with a plain
     parquet write (no partitionBy, no coalesce(1)) to a STABLE path (not
     auto-cleaned by that job).
  2. Add ONE entry to get_reports() below with that path, the output dir,
     partition column, and csv filename. Do not create a new file per report.

Usage:
    python3 polars_csv_export.py                  # run every registered report
    python3 polars_csv_export.py --only cba        # run only reports whose name contains "cba"
    python3 polars_csv_export.py --no-cleanup      # keep the plain parquet after export (debugging)
    python3 polars_csv_export.py --max-rows-per-chunk 20000000   # lower if memory is still tight
"""
import sys
import time
import shutil
import argparse
import resource
from math import ceil
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

sys.path.append(str(Path(__file__).resolve().parents[2]))
# NOTE: these config loaders are plain Python (no pyspark import) - safe to
# reuse here so paths stay in sync with what each Spark job writes, instead
# of hardcoding/duplicating path strings in two places.
from jobs.default_config import create_config
from jobs.config import get_environment_config

# CHANGED: upper bound on rows held in memory at once. The CBA govt export
# (129M rows) completed with the old whole-file approach; ACBP enrolment
# (220M rows) did not. 40M gives a ~3x margin below the size that is known to
# work. If a report has much wider rows than CBA, lower it (--max-rows-per-chunk).
MAX_ROWS_PER_CHUNK = 40_000_000


def _peak_rss_gb() -> float:
    """Peak resident memory of this process so far (Linux reports KiB)."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)


def write_csv_per_mdo_id_polars(parquet_path: str, output_dir: str, partition_col: str,
                                csv_filename: str, max_workers: int,
                                max_rows_per_chunk: int = MAX_ROWS_PER_CHUNK):
    """
    Reads a plain (non-partitioned) parquet dataset and writes one CSV per
    partition_col value, processing the data in bounded-size chunks.

    Rows are assigned to chunks by hashing partition_col, so every value of
    partition_col (every org) lands in exactly one chunk and its CSV is
    written exactly once, with its rows in original order.

    This function does NOT delete anything: main() decides when output dirs
    are cleaned and when input parquet is removed.
    """
    import polars as pl

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # Glob pattern, not the bare directory - Spark writes hidden .crc
    # checksum files alongside the .parquet part files; Polars chokes on
    # those if pointed at the directory directly.
    source = f"{parquet_path}/*.parquet"

    t0 = time.time()
    count_expr = pl.len() if hasattr(pl, "len") else pl.count()  # older Polars fallback
    total_rows = pl.scan_parquet(source).select(count_expr).collect().item()
    n_chunks = max(1, ceil(total_rows / max_rows_per_chunk))
    print(f"   {total_rows:,} rows -> {n_chunks} chunk(s) of ~{ceil(total_rows / max(n_chunks, 1)):,} rows "
          f"(peak memory so far {_peak_rss_gb():.1f} GB)")

    def write_org(item):
        org_value, part_df = item
        # partition_by's dict key is a tuple even for a single grouping column
        safe_org = str(org_value[0] if isinstance(org_value, tuple) else org_value).replace("/", "_")
        folder = out_path / f"{partition_col}={safe_org}"
        folder.mkdir(parents=True, exist_ok=True)
        part_df.write_csv(str(folder / csv_filename))

    rows_written = 0
    groups_written = 0
    for i in range(n_chunks):
        t_chunk = time.time()
        lf = pl.scan_parquet(source)
        if n_chunks > 1:
            # null keys hash to null; fill_null keeps them (bucket 0) instead of silently dropping them
            bucket = pl.col(partition_col).hash().fill_null(0) % pl.lit(n_chunks, dtype=pl.UInt64)
            lf = lf.filter(bucket == pl.lit(i, dtype=pl.UInt64))
        df = lf.collect()
        chunk_rows = df.height
        if chunk_rows == 0:
            continue

        partitions = df.partition_by(partition_col, as_dict=True)
        del df  # free the un-split copy before writing
        n_groups = len(partitions)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(write_org, partitions.items()))
        del partitions

        rows_written += chunk_rows
        groups_written += n_groups
        print(f"   chunk {i + 1}/{n_chunks}: {chunk_rows:,} rows, {n_groups} groups, "
              f"{time.time() - t_chunk:.1f}s (peak memory so far {_peak_rss_gb():.1f} GB)")

    # CHANGED: never report success if rows went missing
    if rows_written != total_rows:
        raise RuntimeError(
            f"Row count mismatch for {parquet_path}: source has {total_rows:,} rows, "
            f"CSV export wrote {rows_written:,}"
        )

    elapsed = time.time() - t0
    print(f"✅ Export took {elapsed:.2f}s total ({elapsed / 60:.2f} min), "
          f"{groups_written} groups, {rows_written:,} rows -> {output_dir}")
    return elapsed


def get_reports(today: str, config):
    """
    ============================================================
    REPORT REGISTRY — add one entry per report here.
    ============================================================
    Keep each entry's "parquet_path" in exact sync with the path that
    report's own Spark job writes to (search that job for the matching
    comment block). Nothing else in this file needs to change when adding
    a new report.
    """
    reports = []

    # ---- Course Based Assessment Report (CBA) ----
    cba_out_path = f"{config.localReportDir}/{config.cbaReportPath}/{today}"

    reports.append({
        "name": "CBA - Govt",
        "parquet_path": f"{config.localReportDir}/temp/cba-report-plain/{today}/govt",
        "output_dir": cba_out_path,
        "partition_col": "mdoid",
        "csv_filename": config.cbaReport,
        "max_workers": 16,
    })
    reports.append({
        "name": "CBA - Non-Govt",
        "parquet_path": f"{config.localReportDir}/temp/cba-report-plain/{today}/non_govt",
        "output_dir": cba_out_path,
        "partition_col": "mdoid",
        "csv_filename": config.cbaReport,
        "max_workers": 16,
    })

    # ---- ACBP: CBP Enrolment Report ----
    reports.append({
        "name": "ACBP - CBP Enrolment Report",
        "parquet_path": f"{config.localReportDir}/temp/cbp-enrolment-report-plain/{today}",
        "output_dir": f"{config.localReportDir}/{config.acbpMdoEnrolmentReportPath}/{today}",
        "partition_col": "mdoid",
        "csv_filename": config.cbpEnrolmentReport,
        "max_workers": 16,
    })

    # ---- ACBP: CBP User Summary Report ----
    reports.append({
        "name": "ACBP - CBP Summary Report",
        "parquet_path": f"{config.localReportDir}/temp/cbp-summary-report-plain/{today}",
        "output_dir": f"{config.localReportDir}/{config.acbpMdoSummaryReportPath}/{today}",
        "partition_col": "mdoid",
        "csv_filename": config.cbpSummaryReport,
        "max_workers": 16,
    })

    # ---- Course Report ----
    reports.append({
        "name": "Course Report",
        "parquet_path": f"{config.localReportDir}/temp/course-report-plain/{today}",
        "output_dir": f"{config.localReportDir}/{config.courseReportPath}/{today}",
        "partition_col": "mdoid",
        "csv_filename": config.courseReport,
        "max_workers": 16,
    })

    # ---- User Enrolment Report ----
    reports.append({
        "name": "User Enrolment Report",
        "parquet_path": f"{config.localReportDir}/temp/user-enrolment-report-plain/{today}",
        "output_dir": f"{config.localReportDir}/{config.userEnrolmentReportPath}/{today}",
        "partition_col": "mdoid",
        "csv_filename": config.userEnrollmentReport,
        "max_workers": 16,
    })

    # ---- ACBP Enrolment Report ----
    reports.append({
        "name": "ACBP Enrolment Report",
        "parquet_path": f"{config.localReportDir}/temp/cbp-enrolment-report-plain/{today}",
        "output_dir": f"{config.localReportDir}/{config.acbpMdoEnrolmentReportPath}/{today}",
        "partition_col": "mdoid",
        "csv_filename": config.cbpEnrolmentReport,
        "max_workers": 16,
    })

    # ---- ACBP Summary Report ----
    reports.append({
        "name": "ACBP Summary Report",
        "parquet_path": f"{config.localReportDir}/temp/cbp-summary-report-plain/{today}",
        "output_dir": f"{config.localReportDir}/{config.acbpMdoSummaryReportPath}/{today}",
        "partition_col": "mdoid",
        "csv_filename": config.cbpSummaryReport,
        "max_workers": 16,
    })

    # ---- Next report goes here — copy the block above and adjust paths ----
    # reports.append({
    #     "name": "...",
    #     "parquet_path": f"{config.localReportDir}/temp/.../{today}",
    #     "output_dir": f"{config.localReportDir}/{config....Path}/{today}",
    #     "partition_col": "mdoid",
    #     "csv_filename": config....,
    #     "max_workers": 16,
    # })

    return reports


def main():
    # CHANGED: flush every line immediately. When this script runs under
    # Airflow its stdout is a pipe (block-buffered by default), so after a
    # SIGKILL the last lines of progress could be lost and point at the wrong step.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(description="Polars per-org CSV export — runs every registered report by default")
    parser.add_argument("--only", help="Run only reports whose name contains this text (case-insensitive)")
    parser.add_argument("--no-cleanup", action="store_true", help="Keep plain parquet files after export (debugging)")
    parser.add_argument("--max-rows-per-chunk", type=int, default=MAX_ROWS_PER_CHUNK,
                        help="Max rows held in memory at once per report (lower = less memory, slightly slower)")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    config_dict = get_environment_config()
    config = create_config(config_dict)

    reports = get_reports(today, config)
    if args.only:
        reports = [r for r in reports if args.only.lower() in r["name"].lower()]
        if not reports:
            print(f"No registered report matches --only '{args.only}'")
            return

    print(f"[START] Polars CSV export — {len(reports)} report(s) queued")
    overall_start = time.time()

    # Reports that share an output_dir (CBA govt + non-govt) are handled as one group.
    groups = {}
    for report in reports:
        groups.setdefault(report["output_dir"], []).append(report)

    failed = []
    skipped = []

    for output_dir, group in groups.items():
        runnable = []
        for report in group:
            if Path(report["parquet_path"]).exists():
                runnable.append(report)
            else:
                print(f"\n⏭️  Skipping {report['name']} — no parquet found at {report['parquet_path']} "
                      f"(the upstream Spark job may not have run, or found no rows for this subset)")
                skipped.append(report["name"])

        # CHANGED: if there is nothing to export, leave the output dir ALONE.
        # The old code wiped it up front regardless, which destroyed finished
        # CBA output on the retry (its input parquet had already been deleted).
        if not runnable:
            print(f"   Output directory left untouched: {output_dir}")
            continue

        # CHANGED: wipe stale output only now, immediately before rewriting it.
        out_path = Path(output_dir)
        if out_path.exists():
            print(f"\n🧹 Cleaning output directory (just before writing): {output_dir}")
            shutil.rmtree(out_path)
        out_path.mkdir(parents=True, exist_ok=True)

        try:
            for report in runnable:
                print(f"\n➡️  {report['name']}: {report['parquet_path']} -> {report['output_dir']}")
                write_csv_per_mdo_id_polars(
                    report["parquet_path"],
                    report["output_dir"],
                    report["partition_col"],
                    report["csv_filename"],
                    report["max_workers"],
                    max_rows_per_chunk=args.max_rows_per_chunk,
                )
        except Exception as e:
            # CHANGED: one failing report no longer stops the rest
            label = " + ".join(r["name"] for r in runnable)
            print(f"\n✗ FAILED: {label}: {type(e).__name__}: {e}")
            failed.append(label)

    print(f"\n[END] All reports processed in {time.time() - overall_start:.2f}s "
          f"(peak memory {_peak_rss_gb():.1f} GB)")
    if skipped:
        print(f"⚠️  Skipped (no input found): {', '.join(skipped)}")

    if failed:
        # CHANGED: keep ALL inputs so the whole job can simply be rerun.
        print(f"✗ {len(failed)} report group(s) failed: {', '.join(failed)}")
        print("   Plain parquet inputs were kept so this job can be rerun safely.")
        sys.exit(1)

    # CHANGED: input parquet is deleted only after EVERYTHING succeeded
    # (previously each report's input was deleted right after its own success).
    if args.no_cleanup:
        print("ℹ️  --no-cleanup set: keeping plain parquet inputs")
    else:
        for report in reports:
            p = Path(report["parquet_path"])
            if p.exists():
                try:
                    shutil.rmtree(p)
                    print(f"🧹 Cleaned up: {p}")
                except Exception as e:
                    print(f"⚠️  Could not clean up {p}: {e}")


if __name__ == "__main__":
    main()
