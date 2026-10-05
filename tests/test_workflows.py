"""Keep the CodeQL workflow and the misthelper-devtools pins from drifting."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # Repository root, from this test file.
WORKFLOWS = ROOT / ".github" / "workflows"  # Folder of each GitHub workflow.
DEVTOOLS_SHA = "da02d4c6a2163d1882f2ad25fce80b8ba38304d1"  # Commit of release v0.6.2.
MAIN_SAFE_CANCEL_VALUE = "${{ github.ref != 'refs/heads/main' }}"  # False on main.
MAIN_SAFE_CANCEL = f"cancel-in-progress: {MAIN_SAFE_CANCEL_VALUE}"  # Full line form.
PIN_PATTERN = re.compile(  # Finds each Git pin of misthelper-devtools with its comment.
    r"misthelper-devtools[^@\s]*@([0-9a-f]{40})\s+#\s*(v[0-9.]+)"
)


def test_every_devtools_pin_names_release_0_6_2() -> None:
    """Each workflow and requirement pin names the v0.6.2 commit and comment."""
    sources = sorted(WORKFLOWS.glob("*.yml")) + [  # Each file that can pin devtools.
        ROOT / "requirements-dev.txt"
    ]
    pins = [  # Each (sha, release) pair across the files.
        pin for source in sources for pin in PIN_PATTERN.findall(source.read_text())
    ]
    assert len(pins) >= 6, pins  # Fails if the pattern stops matching the pins.
    assert set(pins) == {(DEVTOOLS_SHA, "v0.6.2")}  # One commit and one release only.


def test_codeql_workflow_calls_the_shared_analysis() -> None:
    """The CodeQL caller keeps its triggers, scopes, inputs, and pin."""
    text = (WORKFLOWS / "codeql.yml").read_text()  # The caller workflow under test.
    for required in [  # Each value that a future edit can remove by mistake.
        "  pull_request:\n",
        "  push:\n    branches: [main]\n",
        "  schedule:\n",
        "  workflow_dispatch:\n",
        "permissions: {}\n",
        "      actions: read\n",
        "      contents: read\n",
        "      security-events: write\n",
        f"reusable-codeql.yml@{DEVTOOLS_SHA} # v0.6.2\n",
        "      languages: '[\"python\"]'\n",
        "      config-file: ./.github/codeql/codeql-config.yml\n",
    ]:
        assert required in text, required  # Names the absent value on failure.
    assert (ROOT / ".github" / "codeql" / "codeql-config.yml").is_file()


def test_codeql_never_cancels_a_main_run() -> None:
    """A new commit cancels a branch run, but a run on main always completes."""
    text = (WORKFLOWS / "codeql.yml").read_text()  # The caller workflow under test.
    assert "github.head_ref || github.ref" in text  # Branch runs share a group key.
    assert MAIN_SAFE_CANCEL in text  # The cancel flag is false on main.


def test_no_workflow_cancels_a_main_run() -> None:
    """Each workflow that cancels runs keeps the cancel flag false on main."""
    sources = sorted(WORKFLOWS.glob("*.yml"))  # Each workflow of the repository.
    flags = {  # The cancel flag of each workflow that sets one.
        source.name: re.findall(r"cancel-in-progress:\s*(.+)", source.read_text())
        for source in sources
    }
    assert "quality-gates.yml" in flags, flags  # Fails if the scan reads no file.
    for name, values in flags.items():
        for value in values:  # Each flag is false, or false on main.
            assert value.strip() in {"false", MAIN_SAFE_CANCEL_VALUE}, (name, value)
    assert flags["quality-gates.yml"] == [MAIN_SAFE_CANCEL_VALUE]  # Issue #29.
