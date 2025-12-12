#!/usr/bin/env python3
"""
Script to parse MongoDB log files and identify COLLSCAN queries
"""

import re
import json
from datetime import datetime
from collections import defaultdict


def parse_mongo_log(log_file_path):
    """
    Parse MongoDB log file and extract COLLSCAN queries
    """
    print(f"{'=' * 70}")
    print(f"Analyzing MongoDB Log: {log_file_path}")
    print('=' * 70)

    collscan_queries = []
    line_number = 0

    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line_number += 1

                # Look for COLLSCAN in the line
                if 'COLLSCAN' in line:
                    # Try to extract relevant information
                    query_info = parse_collscan_line(line, line_number)
                    if query_info:
                        collscan_queries.append(query_info)

        return collscan_queries

    except FileNotFoundError:
        print(f"✗ Error: File not found - {log_file_path}")
        return []
    except Exception as e:
        print(f"✗ Error reading file: {e}")
        return []


def parse_collscan_line(line, line_number):
    """
    Parse a single log line containing COLLSCAN
    """
    query_info = {
        'line_number': line_number,
        'timestamp': None,
        'collection': None,
        'query': None,
        'duration_ms': None,
        'docs_examined': None,
        'keys_examined': None,
        'plan_summary': None,
        'raw_line': line.strip()
    }

    # Extract timestamp (multiple formats)
    # Format 1: ISO timestamp like 2024-12-10T10:30:45.123+0000
    timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+[+-]\d{4})', line)
    if timestamp_match:
        query_info['timestamp'] = timestamp_match.group(1)

    # Extract namespace (database.collection)
    ns_match = re.search(r'ns:\s*([^\s]+\.[^\s]+)', line)
    if ns_match:
        query_info['collection'] = ns_match.group(1)

    # Extract collection name (alternative pattern)
    if not query_info['collection']:
        collection_match = re.search(r'collection:\s*"?([^"\s]+)"?', line)
        if collection_match:
            query_info['collection'] = collection_match.group(1)

    # Extract query
    query_match = re.search(r'query:\s*({[^}]+})', line)
    if query_match:
        try:
            query_info['query'] = query_match.group(1)
        except:
            query_info['query'] = query_match.group(1)

    # Extract duration
    duration_match = re.search(r'(\d+)ms', line)
    if duration_match:
        query_info['duration_ms'] = int(duration_match.group(1))

    # Extract docs examined
    docs_match = re.search(r'docsExamined:\s*(\d+)', line)
    if docs_match:
        query_info['docs_examined'] = int(docs_match.group(1))

    # Extract keys examined
    keys_match = re.search(r'keysExamined:\s*(\d+)', line)
    if keys_match:
        query_info['keys_examined'] = int(keys_match.group(1))

    # Extract plan summary
    plan_match = re.search(r'planSummary:\s*([^\s]+)', line)
    if plan_match:
        query_info['plan_summary'] = plan_match.group(1)

    return query_info


def print_summary(collscan_queries):
    """
    Print summary statistics
    """
    if not collscan_queries:
        print("\n✓ No COLLSCAN queries found!")
        return

    print(f"\n{'=' * 70}")
    print(f"SUMMARY")
    print('=' * 70)
    print(f"\nTotal COLLSCAN queries found: {len(collscan_queries)}")

    # Group by collection
    by_collection = defaultdict(int)
    total_duration = 0
    slow_queries = []

    for query in collscan_queries:
        if query['collection']:
            by_collection[query['collection']] += 1

        if query['duration_ms']:
            total_duration += query['duration_ms']
            if query['duration_ms'] > 100:  # Consider >100ms as slow
                slow_queries.append(query)

    # Print by collection
    if by_collection:
        print(f"\nCOLLSCAN queries by collection:")
        for collection, count in sorted(by_collection.items(), key=lambda x: x[1], reverse=True):
            print(f"  {collection}: {count}")

    # Print slow queries
    if slow_queries:
        print(f"\nSlow COLLSCAN queries (>100ms): {len(slow_queries)}")
        print(f"Average duration: {total_duration / len(collscan_queries):.2f}ms")


def print_detailed_results(collscan_queries, show_details=True, limit=10):
    """
    Print detailed results
    """
    if not collscan_queries:
        return

    print(f"\n{'=' * 70}")
    print(f"DETAILED RESULTS (showing first {limit})")
    print('=' * 70)

    for i, query in enumerate(collscan_queries[:limit], 1):
        print(f"\n--- Query #{i} (Line {query['line_number']}) ---")

        if query['timestamp']:
            print(f"Timestamp:      {query['timestamp']}")

        if query['collection']:
            print(f"Collection:     {query['collection']}")

        if query['duration_ms'] is not None:
            print(f"Duration:       {query['duration_ms']}ms")

        if query['docs_examined'] is not None:
            print(f"Docs Examined:  {query['docs_examined']}")

        if query['keys_examined'] is not None:
            print(f"Keys Examined:  {query['keys_examined']}")

        if query['plan_summary']:
            print(f"Plan:           {query['plan_summary']}")

        if query['query'] and show_details:
            print(f"Query:          {query['query']}")

        if show_details:
            print(f"Raw:            {query['raw_line'][:100]}...")

    if len(collscan_queries) > limit:
        print(f"\n... and {len(collscan_queries) - limit} more queries")


def export_to_file(collscan_queries, output_file):
    """
    Export results to a file
    """
    try:
        with open(output_file, 'w') as f:
            f.write("MongoDB COLLSCAN Queries Report\n")
            f.write("=" * 70 + "\n\n")

            for i, query in enumerate(collscan_queries, 1):
                f.write(f"Query #{i} (Line {query['line_number']})\n")
                f.write(f"  Timestamp:     {query['timestamp']}\n")
                f.write(f"  Collection:    {query['collection']}\n")
                f.write(f"  Duration:      {query['duration_ms']}ms\n")
                f.write(f"  Docs Examined: {query['docs_examined']}\n")
                f.write(f"  Query:         {query['query']}\n")
                f.write(f"  Raw Line:      {query['raw_line']}\n")
                f.write("\n" + "-" * 70 + "\n\n")

        print(f"\n✓ Results exported to: {output_file}")

    except Exception as e:
        print(f"✗ Error exporting to file: {e}")


def main():
    """Main function"""

    # Configuration - UPDATE THIS
    LOG_FILE_PATH = "C:\\Users\\Myself\\Downloads\\sample_mongodb_log.log"  # Change to your log file path
    EXPORT_RESULTS = True  # Set to True to export to file
    OUTPUT_FILE = "collscan_report.txt"
    SHOW_DETAILS = True  # Show full query details
    LIMIT = 20  # Number of queries to show in detail

    print("\nMongoDB COLLSCAN Query Finder")

    # Parse the log file
    collscan_queries = parse_mongo_log(LOG_FILE_PATH)

    # Print summary
    print_summary(collscan_queries)

    # Print detailed results
    print_detailed_results(collscan_queries, show_details=SHOW_DETAILS, limit=LIMIT)

    # Export if requested
    if EXPORT_RESULTS and collscan_queries:
        export_to_file(collscan_queries, OUTPUT_FILE)

    print(f"\n{'=' * 70}")
    print("Analysis Complete!")
    print('=' * 70)


if __name__ == "__main__":
    main()
