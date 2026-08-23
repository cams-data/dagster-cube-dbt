"""Strategic, name-keyed YAML merging for cube merge-patch files.

Vendored (on top of the `deepmerge` PyPI package) from the algorithm in
https://github.com/john-wd/pyyaml-merger (`yamlmerger/merger.py`), which is unpublished and
pins ancient exact dependency versions. Same behavior: list items are matched by a key field
(default `name`); a patch item with no match is appended, a match is deep-merged unless it
carries `$mergeStrategy: remove` (drop the matched item) or `$mergeStrategy: replace`
(substitute it wholesale). Applies recursively, so nested lists (e.g. `dimensions` within a
`cubes` entry) are merged the same way as top-level ones.

Also supports `$mergeStrategy: patch` (not part of the original vendored algorithm): like the
default (deep-merge) strategy when matched, but raises if no match is found, instead of
silently creating a new, likely-incomplete entry. This exists because a plain merge item
whose target has since been renamed or removed (e.g. the dbt model behind a cube was dropped)
would otherwise silently become a malformed new resource -- missing required fields, and
still carrying any of *its own* nested `$mergeStrategy` keys unprocessed, since those are
only stripped/applied when an item actually goes through the merge machinery, not when it's
appended as-is.
"""

import copy
import warnings
from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

import deepmerge

DEFAULT_MERGE_KEY = "name"
MERGE_STRATEGY_KEY = "$mergeStrategy"
EXTENDS_KEY = "extends"


class CircularExtendsError(ValueError):
    """Raised when a cube's `extends` chain cycles back on itself."""

    def __init__(self, cycle: list[str]) -> None:
        self.cycle = cycle
        super().__init__("Circular `extends` chain: " + " -> ".join(cycle))


class UnmatchedPatchTargetError(ValueError):
    """Raised when a `$mergeStrategy: patch` item matches nothing to patch."""

    def __init__(self, targets: list[str]) -> None:
        self.targets = targets
        offenders = "\n".join(f"  - {name}" for name in targets)
        super().__init__(
            "The following `$mergeStrategy: patch` items did not match anything to patch:\n"
            f"{offenders}\n"
            "Their target may have been renamed, removed, or excluded from generation. Fix "
            "the `name` to match the current target, remove the stale patch, or drop "
            "`$mergeStrategy: patch` if this item is meant to create something new."
        )


class StrategicMerger(deepmerge.Merger):
    def __init__(self, key: str = DEFAULT_MERGE_KEY) -> None:
        self.key = key
        # Collected across every list encountered during a merge (top-level `cubes`/`views`
        # and any nested list, e.g. `dimensions`), so violations from every patch file folded
        # into a single `merge_documents()` call are reported together, not one at a time.
        self.unmatched_patch_targets: list[str] = []
        self.unmatched_remove_targets: list[str] = []
        super().__init__(
            [
                (list, self._merge_list),
                (dict, ["merge"]),
                (set, ["union"]),
            ],
            ["override"],
            ["override"],
        )

    def _merge_list(
        self,
        _merger: "StrategicMerger",
        path: list[Any],
        base_value: list[Any],
        value_to_merge_in: list[Any],
    ) -> list[Any]:
        for element in value_to_merge_in:
            if self.key not in element:
                # an unnamed/unkeyed list: nothing to match against, so treat the whole
                # incoming list as a wholesale replacement.
                return value_to_merge_in

            # Always strip the strategy key before it can reach the output, regardless of
            # which branch below runs -- otherwise a newly-appended (unmatched) element keeps
            # a literal, meaningless `$mergeStrategy` key in the final document.
            strategy = element.pop(MERGE_STRATEGY_KEY, "").lower()
            name = str(element[self.key])

            matches = [
                (index, item)
                for index, item in enumerate(base_value)
                if item.get(self.key) == element[self.key]
            ]
            if not matches:
                if strategy == "remove":
                    # a no-op: the target is already gone either way, so the outcome is the
                    # same as if this had matched and removed it. Likely stale, though --
                    # surfaced as a warning by merge_documents(), not silently ignored.
                    self.unmatched_remove_targets.append(name)
                elif strategy == "patch":
                    # unlike `remove`, this does NOT converge to the same outcome: silently
                    # creating a new entry from a patch fragment produces a malformed one
                    # (missing required fields, unprocessed nested $mergeStrategy keys) --
                    # collected and raised by merge_documents() instead.
                    self.unmatched_patch_targets.append(name)
                else:
                    # default (implicit merge) and `replace` both degrade to a plain add
                    # when there's nothing to merge into or replace.
                    base_value.append(element)
                continue

            index, existing = matches[0]
            if strategy == "remove":
                base_value.remove(existing)
            elif strategy == "replace":
                base_value[index] = element
            else:
                # default (implicit merge) and `patch` behave identically once matched --
                # `patch` only changes what happens when there's no match at all.
                base_value[index] = self.value_strategy(path + [index], existing, element)
        return base_value


def get_strategic_merger(key: str = DEFAULT_MERGE_KEY) -> StrategicMerger:
    return StrategicMerger(key=key)


def merge_documents(
    base: dict[str, Any],
    patches: Iterable[dict[str, Any]],
    key: str = DEFAULT_MERGE_KEY,
) -> dict[str, Any]:
    """Fold `patches` into `base`, in order, using the strategic merge. Mutates and returns `base`.

    Raises `UnmatchedPatchTargetError` if any `$mergeStrategy: patch` item across all of
    `patches` never matched anything (collected across the whole run, not just the first
    one found). Warns (via `warnings.warn`) if any `$mergeStrategy: remove` item never
    matched anything -- safe (the outcome is the same either way) but likely stale.
    """
    merger = get_strategic_merger(key)
    result = base
    for patch in patches:
        result = merger.merge(result, patch)

    if merger.unmatched_patch_targets:
        raise UnmatchedPatchTargetError(sorted(set(merger.unmatched_patch_targets)))
    if merger.unmatched_remove_targets:
        offenders = "\n".join(f"  - {name}" for name in sorted(set(merger.unmatched_remove_targets)))
        warnings.warn(
            "The following `$mergeStrategy: remove` items did not match anything and had no "
            f"effect (their target may already be gone):\n{offenders}",
            stacklevel=2,
        )
    return result


def resolve_extends(
    entities: Sequence[Mapping[str, Any]], key: str = DEFAULT_MERGE_KEY
) -> dict[str, dict[str, Any]]:
    """For every entity (cube) with an `extends: parent_name` field, returns its fully
    resolved fields -- the parent's fields with the entity's own (`extends` aside) folded on
    top, using the same strategic merge as merge-patch application, recursively following
    multi-level `extends` chains. Keyed by `name`, including entities with no `extends` at
    all (returned as-is, for uniform lookup).

    This is *read-only*: it never touches `entities` itself, and its result is meant for
    building things like `AssetSpec` metadata/descriptions/`code_version` from a cube's
    *effective* fields -- not for the YAML actually written for Cube to read, since Cube
    resolves `extends` itself at its own runtime and expects to see it as-is.

    An `extends` target not found among `entities` (e.g. it's a hand-authored cube defined
    outside this pipeline entirely) is left unresolved -- there's no visibility into cube
    definitions this library didn't generate or patch, and Cube itself will still resolve it
    at runtime from the raw, unpatched YAML.

    Raises `CircularExtendsError` if an `extends` chain cycles back on itself.
    """
    by_name = {str(entity[key]): entity for entity in entities}
    resolved: dict[str, dict[str, Any]] = {}

    def _resolve(name: str, chain: tuple[str, ...]) -> dict[str, Any]:
        if name in resolved:
            return resolved[name]
        if name in chain:
            raise CircularExtendsError([*chain, name])

        entity = by_name[name]
        parent_name = entity.get(EXTENDS_KEY)
        if not parent_name or parent_name not in by_name:
            result = copy.deepcopy(dict(entity))
            result.pop(EXTENDS_KEY, None)
        else:
            parent = _resolve(str(parent_name), (*chain, name))
            own_fields = {k: v for k, v in entity.items() if k != EXTENDS_KEY}
            result = get_strategic_merger(key).merge(copy.deepcopy(parent), copy.deepcopy(own_fields))

        resolved[name] = result
        return result

    for entity in entities:
        _resolve(str(entity[key]), ())
    return resolved


def discover_patch_files(root: Path, exclude: Iterable[Path] = ()) -> Iterator[Path]:
    """Yield every `*.yml`/`*.yaml` file under `root`, sorted by path for a deterministic merge order.

    `exclude` paths (and anything under them) are skipped -- e.g. the component's own
    `defs.yaml` and the directories generated cube/view YAML is written to, so generation
    output from a previous run is never re-ingested as a patch.
    """
    excluded_dirs = tuple(p for p in exclude if p.is_dir())
    excluded_files = frozenset(p for p in exclude if not p.is_dir())

    candidates = sorted(
        {*root.rglob("*.yml"), *root.rglob("*.yaml")},
        key=lambda p: p.as_posix(),
    )
    for candidate in candidates:
        if candidate in excluded_files:
            continue
        if any(excluded_dir in candidate.parents for excluded_dir in excluded_dirs):
            continue
        yield candidate
