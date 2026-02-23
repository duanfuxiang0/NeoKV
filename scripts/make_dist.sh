#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/make_dist.sh [--out DIR] [--name NAME] [--with-docs] [--with-tests]

Default bundle includes:
  - (output/bin or build/output/bin)/{neoMeta,neoStore,neo_redis_standalone}
  - conf/
  - LICENSE

Notes:
  - Docs under doc/ are excluded unless --with-docs is set.
  - Tests are excluded unless --with-tests is set.
EOF
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

out_dir="${repo_root}/dist"
bundle_name="neokv"
with_docs="0"
with_tests="0"
bin_dir=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      out_dir="$2"
      shift 2
      ;;
    --name)
      bundle_name="$2"
      shift 2
      ;;
    --with-docs)
      with_docs="1"
      shift
      ;;
    --with-tests)
      with_tests="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

git_sha="nogit"
if command -v git >/dev/null 2>&1 && git -C "${repo_root}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git_sha="$(git -C "${repo_root}" rev-parse --short HEAD 2>/dev/null || echo nogit)"
fi
stamp="$(date -u +%Y%m%dT%H%M%SZ)"

bundle_dir="${out_dir}/${bundle_name}-${stamp}-${git_sha}"
mkdir -p "${bundle_dir}"

require_exec() {
  local path="$1"
  if [[ ! -x "${path}" ]]; then
    echo "error: missing executable: ${path}" >&2
    exit 1
  fi
}

detect_bin_dir() {
  local -a candidates=(
    "${repo_root}/output/bin"
    "${repo_root}/build/output/bin"
  )
  for dir in "${candidates[@]}"; do
    if [[ -x "${dir}/neoMeta" && -x "${dir}/neoStore" && -x "${dir}/neo_redis_standalone" ]]; then
      echo "${dir}"
      return 0
    fi
  done

  cat >&2 <<EOF
error: cannot find built binaries.
expected one of:
  - ${repo_root}/output/bin/{neoMeta,neoStore,neo_redis_standalone}
  - ${repo_root}/build/output/bin/{neoMeta,neoStore,neo_redis_standalone}

tip: build first, e.g.:
  mkdir -p build && cd build && cmake .. && make -j\$(nproc)
EOF
  exit 1
}

copy_dir_if_exists() {
  local src="$1"
  local dst="$2"
  if [[ -e "${src}" ]]; then
    mkdir -p "$(dirname "${dst}")"
    cp -a "${src}" "${dst}"
  fi
}

mkdir -p "${bundle_dir}/bin"
bin_dir="$(detect_bin_dir)"
for bin in neoMeta neoStore neo_redis_standalone; do
  require_exec "${bin_dir}/${bin}"
  cp -a "${bin_dir}/${bin}" "${bundle_dir}/bin/"
done

copy_dir_if_exists "${repo_root}/conf" "${bundle_dir}/conf"
copy_dir_if_exists "${repo_root}/LICENSE" "${bundle_dir}/LICENSE"

if [[ "${with_docs}" == "1" ]]; then
  copy_dir_if_exists "${repo_root}/doc" "${bundle_dir}/doc"
fi

if [[ "${with_tests}" == "1" ]]; then
  copy_dir_if_exists "${repo_root}/tests" "${bundle_dir}/tests"
  copy_dir_if_exists "${repo_root}/test" "${bundle_dir}/test"
  copy_dir_if_exists "${repo_root}/scripts/redis_test.sh" "${bundle_dir}/scripts/redis_test.sh"
fi

if command -v sha256sum >/dev/null 2>&1; then
  (
    cd "${bundle_dir}"
    find . -type f -maxdepth 3 -print0 | sort -z | xargs -0 sha256sum
  ) >"${bundle_dir}/SHA256SUMS"
fi

tarball="${bundle_dir}.tar.gz"
tar -C "${out_dir}" -czf "${tarball}" "$(basename "${bundle_dir}")"

echo "created: ${tarball}"
echo "dir:     ${bundle_dir}"
