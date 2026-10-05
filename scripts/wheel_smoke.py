"""Fresh wheel install and all synthetic tests outside the checkout.

Run with a development environment containing build/pytest: python scripts/wheel_smoke.py dist
"""

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import venv
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dist", type=Path)
    parser.add_argument(
        "--wheelhouse", type=Path, help="offline wheels for declared runtime dependencies"
    )
    parser.add_argument(
        "--offline-test-tools",
        action="store_true",
        help="copy only installed pytest tools into the temporary runner",
    )
    args = parser.parse_args()
    wheels = list(args.dist.resolve().glob("*.whl"))
    if len(wheels) != 1:
        raise ValueError("exactly one wheel required for smoke check")
    directory = Path(tempfile.mkdtemp(prefix="ais-wheel-smoke-"))
    venv.EnvBuilder(with_pip=True).create(directory / "venv")
    python = (
        directory / "venv" / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    )
    options = (
        ["--no-index", "--find-links", str(args.wheelhouse.resolve())] if args.wheelhouse else []
    )
    subprocess.run([str(python), "-m", "pip", "install", *options, str(wheels[0])], check=True)
    test_env = dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
    test_env.pop("PYTHONPATH", None)
    if args.offline_test_tools:
        test_tools = directory / "test-tools"
        test_tools.mkdir()
        for name in ("pytest", "_pytest", "pluggy", "packaging", "iniconfig", "pygments", "py"):
            spec = importlib.util.find_spec(name)
            if spec is None or spec.origin is None:
                raise ValueError(f"installed offline test tool missing: {name}")
            if spec.submodule_search_locations:
                shutil.copytree(
                    Path(spec.origin).parent,
                    test_tools / name,
                    ignore=shutil.ignore_patterns("__pycache__"),
                )
            else:
                shutil.copy2(spec.origin, test_tools / f"{name}.py")
        test_env["PYTHONPATH"] = str(test_tools)
    else:
        subprocess.run([str(python), "-m", "pip", "install", "pytest>=8"], check=True)
    shutil.copytree(root / "tests", directory / "tests")
    # Verify this import resolves to installed wheel files rather than source tree.
    subprocess.run(
        [
            str(python),
            "-c",
            "import ais_toolkit; from pathlib import Path; "
            "assert 'site-packages' in Path(ais_toolkit.__file__).parts; "
            "print(ais_toolkit.__version__)",
        ],
        cwd=directory,
        check=True,
    )
    subprocess.run(
        [str(python), "-m", "pytest", "-q", "tests"], cwd=directory, env=test_env, check=True
    )
    cli = python.parent / ("ais.exe" if sys.platform == "win32" else "ais")
    subprocess.run([str(cli), "inspect-catalog"], cwd=directory, check=True)
    subprocess.run(
        [str(cli), "validate-csv", "tests/fixtures/current_synthetic.csv", "--era", "2025+"],
        cwd=directory,
        check=True,
    )
    print("Fresh outside-checkout wheel checks passed")


if __name__ == "__main__":
    main()
