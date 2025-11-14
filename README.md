# Forecast Downloader

A tiny “fire-and-forget” container that watches the public GeoSphere listing at
`https://public.hub.geosphere.at/datahub/resources/nwp-v1-1h-2500m/filelisting/`,
compares it with what you already have under `/data`, and downloads any missing
AROME forecast NetCDF files for the Alpine region. It is intentionally minimal
so you can run it from cron or any scheduler without extra configuration.

## How it works

- The script is hardcoded to list every `nwp_YYYYMMDDHH.nc` entry exposed by the
  public GeoSphere bucket and downloads any files that aren’t present locally.
- Downloads are stored under `/data/YYYY_MM/` inside the container—mount a host
  directory there to persist the files between runs while keeping months
  separated automatically.
- Each container run performs one sync and exits, making it suitable for
  cron-style scheduling or ad-hoc execution.

## Project structure

```
├── Dockerfile
├── requirements.txt
└── src/
  └── sync.py              # main entrypoint executed by the container
```

## Build the container

```bash
docker build -t forecast-downloader .
```

## Run it

Run the container once to sync the files into `./data` on the host (monthly
subfolders such as `./data/2025_11` will be created automatically):

```bash
docker run --rm \
  -v $(pwd)/data:/data \
  forecast-downloader
```

You can safely re-run the container; it only downloads missing files. Inspect
the monthly log files under `./data/logs/` afterward to see the run history (see
Logging below).

## Schedule with cron

A simple cron entry to run the job every three hours might look like this:

```
0 */3 * * * docker run --rm -v /srv/nwp-data:/data forecast-downloader >> /var/log/forecast-downloader.log 2>&1
```

Adjust the host paths as needed. Because the container exits after one sync you
can also use systemd timers, Kubernetes CronJobs, GitHub Actions, etc.

## Logging

- Every run emits a single summary line containing the UTC start time (rounded
  to minutes) and the list of files downloaded during that run.
- Logs are written both to stdout/stderr (for live monitoring) and to
  `/data/logs/<YYYY_MM>.log`, giving you a rolling monthly history that’s easy
  to scan for failures.

## Notes

- If GeoSphere changes the listing page structure, adapt `src/sync.py`
  accordingly.
