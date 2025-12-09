#!/usr/bin/env python3
"""count_ttl_triples.py

Count the number of triples in an RDF Turtle (.ttl) file.

In Turtle format:
- A triple is terminated by a period '.' or semicolon ';'
- Comments start with '#'
- Blank lines and prefixes (@prefix, @base) are ignored
- Multi-line triples are supported

This script counts actual triples by counting statement terminators (. and ;)
while ignoring comments and empty lines.

Usage examples:
  python count_ttl_triples.py file.ttl
  python count_ttl_triples.py file1.ttl file2.ttl
  python count_ttl_triples.py --verbose file.ttl
"""

from __future__ import annotations
import argparse
import re
import sys
from pathlib import Path
from typing import Iterable


def count_triples_in_ttl(filepath: str | Path, verbose: bool = False) -> int:
    """
    Count the number of RDF triples in a Turtle file.

    Returns the count of triples (terminated by . or ;).
    Handles comments, blank lines, and multi-line statements.
    """
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    if not filepath.is_file():
        raise IsADirectoryError(f"Not a file: {filepath}")

    triple_count = 0
    line_num = 0

    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()

    # Process line by line to remove comments properly
    processed_content = []

    for line in lines:
        line_num += 1

        # Remove comments (simple approach: # not in quotes)
        # Handle both single and double quotes
        in_single_quote = False
        in_double_quote = False
        cleaned = []
        i = 0
        while i < len(line):
            char = line[i]

            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
                cleaned.append(char)
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
                cleaned.append(char)
            elif char == '#' and not in_single_quote and not in_double_quote:
                # Rest of line is comment, stop processing
                break
            else:
                cleaned.append(char)
            i += 1

        line = ''.join(cleaned).rstrip()

        # Skip empty lines
        if not line.strip():
            continue

        # Skip Turtle directives (@prefix, @base)
        if line.strip().startswith('@'):
            continue

        processed_content.append(line)

    # Join all processed lines and count terminators
    content_cleaned = ' '.join(processed_content)

    # Count statement terminators (. and ;)
    # In Turtle: semicolon (;) separates predicates for same subject
    #            period (.) ends a statement
    # Each ; or . counts as one triple/statement end
    for char in content_cleaned:
        if char in '.;':
            triple_count += 1

    if verbose:
        print(f"File: {filepath}")
        print(f"Total terminators found: {triple_count}")

    return triple_count


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Count the number of RDF triples in Turtle (.ttl) file(s)'
    )
    parser.add_argument('paths', nargs='+', help="TTL file paths to analyze")
    parser.add_argument('-v', '--verbose', action='store_true', help='Print detailed information')
    parser.add_argument('-q', '--quiet', action='store_true', help='Only print count for single file')

    args = parser.parse_args(argv)

    exit_code = 0
    results = []

    for path in args.paths:
        try:
            count = count_triples_in_ttl(path, verbose=args.verbose)
            results.append((path, count))
        except FileNotFoundError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            exit_code = 2
        except IsADirectoryError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            exit_code = 3
        except PermissionError:
            print(f"ERROR: Permission denied: {path}", file=sys.stderr)
            exit_code = 4
        except UnicodeDecodeError:
            print(f"ERROR: Cannot decode file (not UTF-8): {path}", file=sys.stderr)
            exit_code = 6
        except Exception as e:
            print(f"ERROR reading {path}: {e}", file=sys.stderr)
            exit_code = 7

    # Print results
    if not results and exit_code == 0:
        return 1

    # If single path and quiet, just print count
    if len(results) == 1 and args.quiet:
        print(results[0][1])
        return exit_code

    for path, count in results:
        print(f"{path}: {count} triples")

    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())

