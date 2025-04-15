# Aldi Price Scraper
Simple Aldi price scraper to allow for open and democratic analysis of everyday grocery price trends over time.

This is an active work in progress, and in its current state only dumps a handful of JSON documents to a directory stamped with the time and store identifier.

# Usage
This is a self-contained [uv](https://github.com/astral-sh/uv) script, so have uv installed then simply run the script from the shell.

# Remarks
This scrapes the product pickup API, and products are around 10-15% more than the in-person prices. Most of the time, the markup is about 10% but not exactly 10%, and there's some cases of 15% markup for cheaper items. Currently there's no obvious pattern but it's a work in progress for finding a way back to the true prices.

# Disclaimer
This repository and its author are not affiliated, associated, authorized, endorsed by, or in any way officially connected with Aldi, or any of its subsidiaries or its affiliates.
