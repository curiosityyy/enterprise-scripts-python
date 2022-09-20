# Enterprise Search Tweets - Make a data or counts request against 30-day or Full-Archive Search
import time
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


def main(query, file):
    endpoint = determine_endpoint()
    # Build request body from file if it exists, else use cli args
    request_body = build_request_body(query)
    # Make the first request
    try:
        first_response = requests.post(url=endpoint, auth=(USERNAME, PASSWORD), json=request_body)
    except requests.exceptions.RequestException as e:
        print(e)
        return main(query, file)
    print(f"Status: {first_response.status_code}\n")
    if first_response.status_code == 429:
        print(f"TooManyRequests, sleeping for 1 minutes")
        time.sleep(60)
        return main(query, file)

    json_response = (json.loads(first_response.text))
    file.write(first_response.text + '\n')
    total_count = json_response.get('totalCount')

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
                return main(query, file)

            print(f"Status: {response.status_code}\n")
            if response.status_code == 429:
                print(f"TooManyRequests, sleeping for 1 minutes")
                time.sleep(60)
                continue

            # Parse n response and it's 'next' token
            n_response = (json.loads(response.text))
            file.write(response.text + '\n')
            total_count += n_response.get('totalCount')
            next_token = n_response.get("next")
            request_count += 1  # Iterates the request counter

        print(f"Done paginating.\nTotal requests made: {request_count}")
    print(f"Total count: {total_count}")
    return total_count


def determine_endpoint():
    domain = "https://gnip-api.twitter.com"
    endpoint = f"{domain}/search/{ARCHIVE}/accounts/{ACCOUNT_NAME}/{ENDPOINT_LABEL}/counts.json"

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


def read_twitter_user_details():
    n_batch = 0
    n_user = 0
    state_file = open("states.txt", "r")
    total_tweets = 0
    start_idx = len(state_file.readlines())
    state_file.close()
    print("Starting at index: " + str(start_idx))
    state_file = open("states.txt", "a")
    with open("twitter_user_details.json", "r") as read_file:
        batch = []
        for line in read_file.readlines():
            twitter_user = json.loads(line)
            n_user += 1
            batch.append("from:" + twitter_user.get('id'))
            if n_user % 10 == 0:
                query = ' OR '.join(batch)
                batch = []
                n_batch += 1
                if n_batch < start_idx:
                    continue
                if n_batch > 1:
                    break
                result_file = open(f"tweet_count_batch_{str(n_batch)}", "a")
                print("Starting get counts of " + str(n_batch) + "th user")
                print(query)
                user_tweet_count = main(query, result_file)
                total_tweets += user_tweet_count
                state_file.write(str(batch) + '\n')
        if len(batch) > 0:
            query = ' OR '.join(batch)
            batch = []
            n_batch += 1
            if n_batch >= start_idx:
                result_file = open(f"tweet_count_batch_{str(n_batch)}", "a")
                print("Starting get counts of " + str(n_batch) + "th user")
                print(query)
                user_tweet_count = main(query, result_file)
                total_tweets += user_tweet_count
                state_file.write(str(batch) + '\n')
        print(f"Total users: {n_user}, Total tweets: {total_tweets}")


read_twitter_user_details()
