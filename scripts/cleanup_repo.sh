#!/usr/bin/env bash
# Repository housekeeping script. Idempotent by design.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

log() {
    printf '[cleanup] %s\n' "$*"
}

remove_paths() {
    local description="$1"
    shift
    local -a patterns=("$@")
    local -i removed=0
    for pattern in "${patterns[@]}"; do
        while IFS= read -r -d '' item; do
            rm -rf "$item"
            log "removed ${item#$REPO_ROOT/}"
            ((removed++))
        done < <(find "$REPO_ROOT" -path "$REPO_ROOT/.git" -prune -o -name "$pattern" -print0)
    done
    log "$description: $removed entries cleaned"
}

dedupe_untracked() {
    if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        log "git repository not detected, skip duplicate detection"
        return
    fi

    declare -A tracked_hashes=()
    while IFS= read -r -d '' tracked_file; do
        local hash
        hash=$(sha256sum "$tracked_file" | cut -d' ' -f1)
        tracked_hashes["$hash"]="$tracked_file"
    done < <(git ls-files -z)

    declare -A seen_untracked_hashes=()
    declare -i duplicate_removed=0
    while IFS= read -r -d '' untracked_file; do
        case "$(basename "$untracked_file")" in
            .env|.env.local|.envrc)
                continue
                ;;
        esac
        local hash
        hash=$(sha256sum "$untracked_file" | cut -d' ' -f1)
        if [[ -n "${tracked_hashes[$hash]-}" ]]; then
            log "removing duplicate of ${tracked_hashes[$hash]#$REPO_ROOT/}: ${untracked_file#$REPO_ROOT/}"
            rm -f "$untracked_file"
            ((duplicate_removed++))
        elif [[ -n "${seen_untracked_hashes[$hash]-}" ]]; then
            log "removing duplicate copy: ${untracked_file#$REPO_ROOT/} (original ${seen_untracked_hashes[$hash]#$REPO_ROOT/})"
            rm -f "$untracked_file"
            ((duplicate_removed++))
        else
            seen_untracked_hashes["$hash"]="$untracked_file"
        fi
    done < <(git ls-files -o --exclude-standard -z)
    log "duplicate files removed: $duplicate_removed"
}

cleanup_queue_dirs() {
    local queue_dir="${QUEUE_DIR:-$REPO_ROOT/inbox}"
    local processed_dir="${PROCESSED_DIR:-$REPO_ROOT/processed}"
    local failed_dir="${FAILED_DIR:-$REPO_ROOT/failed}"
    local -a dirs=("$queue_dir" "$processed_dir" "$failed_dir" "$queue_dir/.tmp")
    for dir in "${dirs[@]}"; do
        [[ -d "$dir" ]] || continue
        while IFS= read -r -d '' junk; do
            rm -f "$junk"
            log "removed artefact ${junk#$REPO_ROOT/}"
        done < <(find "$dir" -maxdepth 1 -type f \( -name '*.pyc' -o -name '*.log' -o -name '*.tmp' -o -name '*.json.tmp' \) -print0)
        while IFS= read -r -d '' cache_dir; do
            rm -rf "$cache_dir"
            log "removed cache ${cache_dir#$REPO_ROOT/}"
        done < <(find "$dir" -maxdepth 1 -type d -name '__pycache__' -print0)
    done
}

log "start repo cleanup"

remove_paths "bytecode & cache" "__pycache__" "*.pyc" "*.pyo" "*.py.class"
remove_paths "python tooling caches" ".mypy_cache" ".pytest_cache" ".ruff_cache" ".coverage" "coverage.xml" "htmlcov" "*.egg-info" "*.egg"
remove_paths "editor artefacts" "*~" "*.swp" "*.swo" "*.tmp" "*.tmp.*" ".DS_Store"
remove_paths "logs & build artefacts" "*.log" "*.bak" "*.orig" "*.rej"

dedupe_untracked
cleanup_queue_dirs

log "cleanup finished"
