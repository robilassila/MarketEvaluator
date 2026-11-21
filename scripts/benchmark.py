import argparse
import csv
import json
import math
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RESULTS = REPO / "results"
DATA = REPO / "data"

JOB_REPORT = re.compile(r"Processed (\d+) events in ([\d.]+) s \((\d+) events/s\)")
PRODUCER_REPORT = re.compile(r"Sent (\d+) events to \S+ in ([\d.]+) s \((\d+) events/s\)")
# Must match evaluator.Main.RUNNING.
JOB_RUNNING = "Job running"
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
SIZE = re.compile(r"([\d.]+)\s*([A-Za-z]*)")
SIZE_UNITS = {
    "b": 1, "kb": 1e3, "mb": 1e6, "gb": 1e9, "tb": 1e12,
    "kib": 2**10, "mib": 2**20, "gib": 2**30, "tib": 2**40,
}
RESOURCE_FIELDS = ["container", "elapsed_s", "cpu_percent", "memory_bytes"]


# Parsing; pure functions, covered by scripts/test_benchmark.py.

def parse_size(text: str) -> float:
    """Converts a docker stats size such as "1.5GiB" or "300kB" to bytes."""
    match = SIZE.fullmatch(text.strip())
    if not match or match.group(2).lower() not in SIZE_UNITS:
        raise ValueError(f"Unrecognised size: {text!r}")
    return float(match.group(1)) * SIZE_UNITS[match.group(2).lower()]


def parse_stats_line(line: str) -> tuple[str, float, float] | None:
    """Returns (container name, CPU %, memory bytes) for one `docker stats` JSON line, else None."""
    line = ANSI_ESCAPE.sub("", line).strip()
    if not line.startswith("{"):
        return None
    try:
        stats = json.loads(line)
        cpu = float(stats["CPUPerc"].rstrip("%"))
        memory = parse_size(stats["MemUsage"].split("/")[0])
    except (ValueError, KeyError):
        # Containers that are starting or stopping report "--".
        return None
    return stats["Name"], cpu, memory


def parse_job_report(output: str) -> tuple[int, float, int]:
    """Returns (events, seconds, events/s) from the job's final report line."""
    match = JOB_REPORT.search(output)
    if not match:
        raise RuntimeError(f"No throughput report in the job's output:\n{output}")
    return int(match.group(1)), float(match.group(2)), int(match.group(3))


def parse_producer_report(output: str) -> tuple[int, int]:
    """Returns (events sent, achieved events/s) from the producer's final report line."""
    match = PRODUCER_REPORT.search(output)
    if not match:
        raise RuntimeError(f"No report in the producer's output:\n{output}")
    return int(match.group(1)), int(match.group(3))


def percentile(sorted_values: list[float], quantile: float) -> float:
    """Nearest-rank percentile, the same definition as evaluator.consumer.Subscriber.percentile."""
    rank = math.ceil(quantile * len(sorted_values))
    return sorted_values[min(len(sorted_values) - 1, max(rank, 1) - 1)]


# Docker.

def docker(*args: str, capture: bool = False, check: bool = True) -> str:
    result = subprocess.run(["docker", *args], cwd=REPO, check=check, text=True,
                            stdout=subprocess.PIPE if capture else None)
    return result.stdout or ""


def run_main(*args: str, name: str | None = None, detach: bool = False) -> str:
    """Runs an evaluator program in a container. Paths must be relative to the repository root and
    lie under data/ or results/, the only directories mounted into the container."""
    options = ["--detach"] if detach else ["--rm"]
    if name:
        options += ["--name", name]
    command = ["compose", "run", *options, "-T", "-e", "LOG_LEVEL=error", "evaluator", *args]
    if detach:
        docker(*command, capture=True)
        return ""
    output = docker(*command, capture=True)
    print(output, end="", flush=True)
    return output


def kafka_topics(*args: str) -> None:
    docker("compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
           "--bootstrap-server", "kafka:19092", *args, capture=True)


def delete_topics(*topics: str) -> None:
    for topic in topics:
        docker("compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
               "--bootstrap-server", "kafka:19092", "--delete", "--if-exists", "--topic", topic,
               capture=True, check=False)


def prepare() -> str:
    """Builds the image, starts Kafka and returns the Kafka container's name."""
    docker("compose", "build", "evaluator")
    docker("compose", "up", "--detach", "--wait", "kafka")
    return docker("compose", "ps", "--format", "{{.Name}}", "kafka", capture=True).strip()


def write_host(directory: Path) -> None:
    info = json.loads(docker("info", "--format", "{{json .}}", capture=True))
    (directory / "host.json").write_text(json.dumps(
        {"cpus": info["NCPU"], "memory_bytes": info["MemTotal"]}, indent=2) + "\n")


def remove_container(name: str) -> None:
    docker("rm", "--force", name, capture=True, check=False)


def wait_until_running(container: str, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        logs = subprocess.run(["docker", "logs", container], text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout
        if JOB_RUNNING in logs:
            return
        state = docker("inspect", "--format", "{{.State.Running}}", container, capture=True).strip()
        if state != "true":
            raise RuntimeError(f"Job container {container} exited before running:\n{logs}")
        time.sleep(0.5)
    raise RuntimeError(f"Job container {container} did not report running within {timeout:.0f} s")


class ResourceSampler:
    """Collects `docker stats` samples for the given containers while the `with` block runs.

    `containers` maps container names to the labels written to the CSV. Containers that start
    later are picked up, because `docker stats` without arguments follows new containers.
    """

    def __init__(self, containers: dict[str, str]):
        self.containers = containers
        self.samples: list[dict[str, object]] = []
        self._process: subprocess.Popen | None = None
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "ResourceSampler":
        self._started = time.monotonic()
        self._process = subprocess.Popen(["docker", "stats", "--format", "{{json .}}"],
                                         stdout=subprocess.PIPE, text=True)
        self._thread = threading.Thread(target=self._read, daemon=True)
        self._thread.start()
        return self

    def _read(self) -> None:
        for line in self._process.stdout:
            parsed = parse_stats_line(line)
            if parsed and parsed[0] in self.containers:
                name, cpu, memory = parsed
                self.samples.append({
                    "container": self.containers[name],
                    "elapsed_s": round(time.monotonic() - self._started, 2),
                    "cpu_percent": cpu,
                    "memory_bytes": int(memory),
                })

    def __exit__(self, *exception) -> None:
        self._process.terminate()
        self._process.wait()
        self._thread.join()


def csv_writer(path: Path, fields: list[str]):
    handle = path.open("w", newline="")
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    return handle, writer


# Benchmarks.

def throughput(arguments: argparse.Namespace) -> None:
    kafka = prepare()
    directory = RESULTS / "throughput"
    directory.mkdir(parents=True, exist_ok=True)
    write_host(directory)
    runs_file, runs = csv_writer(directory / "throughput.csv",
                                 ["parallelism", "repetition", "events", "seconds", "events_per_second"])
    resources_file, resources = csv_writer(directory / "resources.csv",
                                           ["parallelism", "repetition", *RESOURCE_FIELDS])
    with runs_file, resources_file:
        for parallelism in arguments.parallelisms:
            for repetition in range(1, arguments.repeat + 1):
                run = f"bench-p{parallelism}-r{repetition}-{int(time.time())}"
                job = f"{run}-job"
                try:
                    with ResourceSampler({kafka: "kafka", job: "job"}) as sampler:
                        report = run_main("job", "--bounded", "--parallelism", str(parallelism),
                                          "--indicators-topic", f"{run}-indicators",
                                          "--advisories-topic", f"{run}-advisories", name=job)
                finally:
                    remove_container(job)
                    delete_topics(f"{run}-indicators", f"{run}-advisories")
                events, seconds, rate = parse_job_report(report)
                key = {"parallelism": parallelism, "repetition": repetition}
                runs.writerow({**key, "events": events, "seconds": seconds, "events_per_second": rate})
                resources.writerows({**key, **sample} for sample in sampler.samples)
                runs_file.flush()
                resources_file.flush()
    print(f"Wrote {directory.relative_to(REPO)}/")


def latency(arguments: argparse.Namespace) -> None:
    inputs = []
    for file in arguments.files:
        path = Path(file).resolve()
        if not path.is_relative_to(DATA):
            sys.exit(f"{file} is not under {DATA}, the only input directory mounted into the container")
        inputs.append(str(path.relative_to(REPO)))

    kafka = prepare()
    directory = RESULTS / "latency"
    directory.mkdir(parents=True, exist_ok=True)
    write_host(directory)
    summary_file, summary = csv_writer(directory / "summary.csv", [
        "target_rate", "achieved_rate", "events", "windows", "p50_ms", "p95_ms", "p99_ms", "max_ms"])
    resources_file, resources = csv_writer(directory / "resources.csv", ["target_rate", *RESOURCE_FIELDS])
    with summary_file, resources_file:
        for rate in arguments.rates:
            run = f"latency-r{rate}-{int(time.time())}"
            job = f"{run}-job"
            producer = f"{run}-producer"
            topics = {kind: f"{run}-{kind}" for kind in ("ticks", "indicators", "advisories")}
            output = directory / f"rate-{rate}"
            output.mkdir(exist_ok=True)
            relative = output.relative_to(REPO)
            try:
                # The job refuses to start without its input topic, and must run before the replay.
                kafka_topics("--create", "--topic", topics["ticks"], "--partitions", str(arguments.partitions),
                             "--config", "retention.ms=-1")
                run_main("job", "--parallelism", str(arguments.parallelism),
                         "--ticks-topic", topics["ticks"], "--indicators-topic", topics["indicators"],
                         "--advisories-topic", topics["advisories"], name=job, detach=True)
                wait_until_running(job)
                with ResourceSampler({kafka: "kafka", job: "job", producer: "producer"}) as sampler:
                    report = run_main("producer", "--topic", topics["ticks"],
                                      "--partitions", str(arguments.partitions), "--rate", str(rate),
                                      "--boundary-log", f"{relative}/boundaries.csv", *inputs, name=producer)
                (output / "producer.log").write_text(report)
                # Live, the last windows never fire, so the job's output goes quiet once it has drained.
                run_main("subscriber", "--indicators-topic", topics["indicators"],
                         "--advisories-topic", topics["advisories"],
                         "--boundary-log", f"{relative}/boundaries.csv",
                         "--idle-timeout", str(arguments.drain_timeout), "--output-dir", str(relative))
                docker("stop", job, capture=True)
                logs = subprocess.run(["docker", "logs", job], text=True,
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout
                (output / "job.log").write_text(logs)
            finally:
                remove_container(job)
                remove_container(producer)
                delete_topics(*topics.values())

            events, achieved = parse_producer_report(report)
            latencies = sorted(float(row["latency_millis"])
                               for row in csv.DictReader((output / "latency.csv").open()))
            if not latencies:
                raise RuntimeError(f"No indicator in {relative} could be matched to the boundary log")
            summary.writerow({
                "target_rate": rate, "achieved_rate": achieved, "events": events, "windows": len(latencies),
                "p50_ms": percentile(latencies, 0.50), "p95_ms": percentile(latencies, 0.95),
                "p99_ms": percentile(latencies, 0.99), "max_ms": latencies[-1],
            })
            resources.writerows({"target_rate": rate, **sample} for sample in sampler.samples)
            summary_file.flush()
            resources_file.flush()
    print((directory / "summary.csv").read_text())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(required=True)

    command = commands.add_parser("throughput", help="resource scaling: bounded job at several parallelisms")
    command.add_argument("--parallelisms", type=int, nargs="+", default=[1, 2, 4, 8, 16])
    command.add_argument("--repeat", type=int, default=3)
    command.set_defaults(run=throughput)

    command = commands.add_parser("latency", help="workload scaling: live job at several input rates")
    command.add_argument("--rates", type=int, nargs="+", default=[50000, 100000, 200000, 400000])
    command.add_argument("--parallelism", type=int, default=4)
    command.add_argument("--partitions", type=int, default=16)
    command.add_argument("--drain-timeout", type=int, default=10, metavar="SECONDS")
    command.add_argument("files", nargs="+", metavar="csv-file")
    command.set_defaults(run=latency)

    arguments = parser.parse_args()
    try:
        arguments.run(arguments)
    except subprocess.CalledProcessError as error:
        sys.exit(f"Command failed with exit code {error.returncode}: {' '.join(error.cmd)}")


if __name__ == "__main__":
    main()
