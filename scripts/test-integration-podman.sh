#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -z "${DOCKER_HOST:-}" ]]; then
  runtime_dir="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
  podman_socket="$runtime_dir/podman/podman.sock"
  if [[ ! -S "$podman_socket" ]]; then
    printf 'Rootless Podman API socket not found: %s\n' "$podman_socket" >&2
    printf 'Start the user Podman socket and rerun this script.\n' >&2
    exit 1
  fi
  export DOCKER_HOST="unix://$podman_socket"
fi

case "$DOCKER_HOST" in
  unix://*/podman/podman.sock)
    socket_path="${DOCKER_HOST#unix://}"
    if [[ ! -S "$socket_path" ]]; then
      printf 'Podman API socket is not available: %s\n' "$socket_path" >&2
      exit 1
    fi
    export TESTCONTAINERS_RYUK_DISABLED=true
    ;;
  *)
    printf 'Integration tests require the rootless Podman API socket; DOCKER_HOST is not a Podman socket.\n' >&2
    exit 1
    ;;
esac

exec go test -race -tags=integration -count=1 "$@" ./...
