#!/usr/bin/python

#
#   Copyright 2024 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

"""Scripts for automatic management of aliases."""

import importlib
import sys
from pathlib import Path


SOURCES = [
    "hopsworks/__init__.py",
    "hopsworks/connection.py",
    "hopsworks/internal",
    "hopsworks/platform",
    "hopsworks/fs",
    "hopsworks/ml",
]
IGNORED = [
    "tests",
    "hsfs",
    "hopsworks",
    "hsml",
    "hopsworks_common",
    "hopsworks.egg-info",
]
# Everything that is not a top-level file, a part of sources, or a part of ignored is considered to be autmoatically managed.


def traverse(path, f):
    if not path.exists():
        return
    if path.is_file():
        f(path)
        return
    for child in path.iterdir():
        traverse(child, f)


def collect_imports(root):
    imports = []

    def imports_add(file):
        pkg = str(file.parent.relative_to(root)).replace("/", ".")
        if file.name == "__init__.py":
            imports.append(pkg)
        elif file.suffix == ".py":
            imports.append(pkg + "." + file.name[:-3])

    for source in SOURCES:
        traverse(root / source, imports_add)

    return imports


def collect_aliases(root):
    for import_str in collect_imports(root):
        importlib.import_module(import_str, package=".")
    aliases = importlib.import_module("hopsworks.internal.aliases", package=".")
    return aliases.Registry.get_modules()


def collect_managed(root):
    managed = {}
    for pkg, imports in collect_aliases(root).items():
        pkg = root / pkg.replace(".", "/") / "__init__.py"
        managed[pkg] = (
            "# ruff: noqa\n"
            "# This file is generated by aliases.py. Do not edit it manually!\n"
        )
        if not imports:
            continue
        managed[pkg] += "import hopsworks.internal.aliases\n"
        imports.sort()  # this is needed for determinism
        imported_modules = {"hopsworks.internal.aliases"}
        declared_names = set()
        for f, i, im in imports:
            original = f"{f}.{i}"
            alias = im.as_alias if im.as_alias else i
            if alias in declared_names:
                print(
                    f"Error: {original} is attempted to be exported as {alias} in {pkg}, "
                    "but the package already contains this alias."
                )
                exit(1)
            if f not in imported_modules:
                managed[pkg] += f"import {f}\n"
                imported_modules.add(f)
            if im.deprecated:
                available_until = ""
                if im.available_until:
                    available_until = f'available_until="{im.available.until}"'
                original = f"hopsworks.internal.aliases.deprecated({available_until})({original})"
            managed[pkg] += f"{alias} = {original}\n"
    return managed


def fix(root):
    managed = collect_managed(root)
    for filepath, content in managed.items():
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.touch()
        filepath.write_text(content)
    ignored = [root / path for path in SOURCES + IGNORED]

    def remove_if_excess(path):
        if path.parent == root:
            return
        if any(path.is_relative_to(p) for p in ignored):
            return
        if path not in managed:
            path.unlink()

    traverse(root, remove_if_excess)


def check(root):
    global ok
    ok = True
    managed = collect_managed(root)
    for filepath, content in managed.items():
        if not filepath.exists():
            print(f"Error: {filepath} should exist.")
            ok = False
            continue
        if filepath.read_text() != content:
            print(f"Error: {filepath} has wrong content.")
            ok = False
    ignored = [root / path for path in SOURCES + IGNORED]

    def check_file(path):
        global ok
        if path.parent == root or any(path.is_relative_to(p) for p in ignored):
            return
        if path.suffix == ".pyc" or "__pycache__" in path.parts:
            return
        if path not in managed:
            print(f"Error: {path} shouldn't exist.")
            ok = False

    traverse(root, check_file)

    if ok:
        print("The aliases are correct!")
    else:
        print("To fix the errors, run `aliases.py fix`.")
        exit(1)


def help(msg=None):
    if msg:
        print(msg + "\n")
    print("Use `aliases.py fix [path]` or `aliases.py check [path]`.")
    print(
        "`path` is optional, current directory (or its `python` subdirectory) is used by default; it should be the directory containing the hopsworks package, e.g., `./python/`."
    )
    exit(1)


def main():
    if len(sys.argv) == 3:
        root = Path(sys.argv[2])
    elif len(sys.argv) == 2:
        root = Path()
        if not (root / "hopsworks").exists():
            root = root / "python"
    else:
        help("Wrong number of arguments.")

    root = root.resolve()
    if not (root / "hopsworks").exists():
        help("The used path doesn't contain the hopsworks package.")

    cmd = sys.argv[1]
    if cmd in ["f", "fix"]:
        cmd = fix
    elif cmd in ["c", "check"]:
        cmd = check
    else:
        help("Unknown command.")

    cmd(root)


if __name__ == "__main__":
    main()
