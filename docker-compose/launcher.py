import argparse
import os
import subprocess
import sys


def get_ingestion_services():
    """
    List all directories that start with 'ingestion-' and contain a docker-compose.yml file.
    """
    ingestion_services = []
    for root, dirs, files in os.walk(""):
        for dir_name in dirs:
            if dir_name.startswith("ingestion-"):
                compose_file = os.path.join(root, dir_name, "docker-compose.yml")
                if os.path.isfile(compose_file):
                    ingestion_services.append(compose_file)
    print(f"Found {len(ingestion_services)} ingestion services.")
    return ingestion_services


def run_docker_compose(compose_files, command, no_cache=False):
    """
    Run docker-compose command for specified compose files.

    :param compose_files: List of paths to docker-compose.yml files.
    :param command: The docker-compose command to run ('up', 'down', 'build').
    :param no_cache: Whether to use --no-cache option with 'build' command.
    """
    for compose_file in compose_files:
        print(f"Running '{command}' for {compose_file}")

        cmd = ["docker-compose", "-f", compose_file, command]
        if command == "build" and no_cache:
            cmd.append("--no-cache")
        if command == "up":
            cmd.append("-d")

        try:
            subprocess.run(cmd, check=True)
            print(f"Successfully executed '{command}' for {compose_file}")
        except subprocess.CalledProcessError as e:
            print(
                f"Failed to execute '{command}' for {compose_file}: {e}",
                file=sys.stderr,
            )


def main():
    parser = argparse.ArgumentParser(
        description="Manage Docker Compose services that start with 'ingestion-'."
    )
    parser.add_argument(
        "action",
        choices=["up", "down", "build", "build-no-cache"],
        help="Docker Compose action to perform.",
    )
    parser.add_argument(
        "--services",
        type=int,
        nargs="+",
        help="Indices of services to run, as space-separated values.",
    )

    args = parser.parse_args()

    # Get the list of ingestion services
    ingestion_services = get_ingestion_services()

    if not ingestion_services:
        print("No 'ingestion-' services found with docker-compose.yml files.")
        return

    # Display the available services
    print("Available ingestion services:")
    for idx, service in enumerate(ingestion_services):
        print(f"{idx + 1}: {service}")

    # Validate selected services
    if args.services:
        selected_services = [
            ingestion_services[idx - 1]
            for idx in args.services
            if 1 <= idx <= len(ingestion_services)
        ]
    else:
        selected_services = ingestion_services

    if not selected_services:
        print("No valid services selected.")
        return

    # Determine if we need no-cache
    no_cache = args.action == "build-no-cache"
    if no_cache:
        action = "build"
    else:
        action = args.action

    # Run the selected action
    run_docker_compose(selected_services, action, no_cache)


if __name__ == "__main__":
    main()
