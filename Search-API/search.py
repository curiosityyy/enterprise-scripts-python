# Enterprise Search Tweets - Make a data or counts request against 30-day or Full-Archive Search
import argparse
import json
import os
import sys

import requests

# Argparse for cli options. Run `python search.py -h` to see the list of arguments.
parser = argparse.ArgumentParser()
parser.add_argument("-r", "--request_file", help="Use json file for request body",
                    action="store_true")
parser.add_argument("-q", "--query", help="A valid query up to 2,048 characters")
parser.add_argument("-c", "--counts", help="Make request to 'counts' endpoint", action="store_true")
parser.add_argument("-f", "--from_date", help="Oldest date from which results will be provided")
parser.add_argument("-t", "--to_date", help="Most recent date to which results will be provided")
parser.add_argument("-m", "--max_results", help="Maximum number of results returned by a single\
                    request/response cycle (range: 10-500, default: 100)")
parser.add_argument("-b", "--bucket", choices=['day', 'hour', 'minute'],
                    help="The unit of time for which count data will be provided.")
parser.add_argument("-n", "--next", help="Auto paginate through next tokens", action="store_true")
parser.add_argument("-pp", "--pretty_print", help="Pretty print the results", action="store_true")
parser.add_argument("-u", "--username", help="Username for Basic Auth")
parser.add_argument("-p", "--password", help="Password for Basic Auth")
parser.add_argument("-a", "--account_name", help="Account name for the search endpoint")
parser.add_argument("-l", "--search_label", help="Search label for the search endpoint")
parser.add_argument("-sa", "--search_archive", choices=['30day', 'fullarchive'],)
args = parser.parse_args()

if not args.username:
    Exception("Username is required.")

if not args.password:
    Exception("Password is required.")

if not args.account_name:
    Exception("Account name is required.")

if not args.search_archive:
    Exception("Search archive is required.")

if not args.search_label:
    Exception("Search label is required.")

USERNAME = args.username
PASSWORD = args.password
ACCOUNT_NAME = args.account_name
ARCHIVE = args.search_archive
ENDPOINT_LABEL = args.search_label


def main():
    endpoint = determine_endpoint()
    # Build request body from file if it exists, else use cli args
    if args.request_file is True:
        request_body = build_request_from_file("request.json")
    else:
        request_body = build_request_body(args.query)
    # Make the first request
    try:
        first_response = requests.post(url=endpoint, auth=(USERNAME, PASSWORD), json=request_body)
    except requests.exceptions.RequestException as e:
        print(e)
        sys.exit(120)
    print(f"Status: {first_response.status_code}\n", format_response(first_response), "\n")
    json_response = (json.loads(first_response.text))
    print(json_response.get('results'))

    # Pagination logic (if -n flag is passed, paginate through the results)
    if json_response.get("next") is None or args.next is False:
        print(f"Request complete.")
    elif json_response.get("next") is not None and args.next:
        next_token = json_response.get("next")
        request_count = 1  # Keep track of the number of requests being made (pagination) 
        while next_token is not None:
            # Update request_body with next token
            request_body.update(next=next_token)
            # Make the request with the next token
            try:
                response = requests.post(url=endpoint, auth=(USERNAME, PASSWORD), json=request_body)
            except requests.exceptions.RequestException as e:
                print(e)
                sys.exit(120)
            print(format_response(response), "\n")
            # Parse n response and it's 'next' token
            n_response = (json.loads(response.text))
            next_token = n_response.get("next")
            request_count += 1  # Iterates the request counter

        print(f"Done paginating.\nTotal requests made: {request_count}")


def determine_endpoint():
    domain = "https://gnip-api.twitter.com"
    if args.counts:
        endpoint = f"{domain}/search/{ARCHIVE}/accounts/{ACCOUNT_NAME}/{ENDPOINT_LABEL}/counts.json"
    else:
        endpoint = f"{domain}/search/{ARCHIVE}/accounts/{ACCOUNT_NAME}/{ENDPOINT_LABEL}.json"

    print(f"Endpoint: {endpoint}")
    return endpoint


def build_request_body(query):
    # Initialize request body with Tweet IDs and groupings (unowned metrics by default)
    request_body = {"query": query}
    if args.from_date:
        request_body.update(fromDate=args.from_date)
    if args.to_date:
        request_body.update(toDate=args.to_date)
    if args.max_results:
        request_body.update(maxResults=args.max_results)
    if args.bucket:
        request_body.update(bucket=args.bucket)

    return request_body


def build_request_from_file(request_file):
    with open("request.json", "r") as read_file:
        request_body = json.load(read_file)

    return request_body


def format_response(response):
    if args.pretty_print:
        formatted_response = json.dumps(json.loads(response.text), indent=2, sort_keys=True)
    else:
        formatted_response = response.text

    return formatted_response


if __name__ == '__main__':
    main()
