'''
Yahoo! Earnings Calendar scraper
'''
import datetime
from datetime import datetime as dt, timezone
import json
import logging
import re
import requests
import time

BASE_URL = 'https://finance.yahoo.com/calendar/earnings'
BASE_STOCK_URL = 'https://finance.yahoo.com/quote'
RATE_LIMIT = 2000.0
SLEEP_BETWEEN_REQUESTS_S = 60 * 60 / RATE_LIMIT
OFFSET_STEP = 100

# Logging config
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.ERROR)


class YahooEarningsCalendar(object):
    """
    This is the class for fetching earnings data from Yahoo! Finance
    """

    def __init__(self, delay=SLEEP_BETWEEN_REQUESTS_S):
        self.delay = delay

    def _get_data_dict(self, url):
        """Fetches data from Yahoo Finance page and extracts earnings data.
        
        The new Yahoo Finance website uses SvelteKit and embeds data in 
        <script type="application/json" data-sveltekit-fetched ...> tags.
        This method parses these tags to extract earnings data.
        """
        time.sleep(self.delay)
        page = requests.get(url)
        page_content = page.content.decode(encoding='utf-8', errors='strict')
        
        # Extract earnings data from data-sveltekit-fetched script tags
        earnings_data = self._extract_earnings_from_sveltekit(page_content)
        if earnings_data:
            return earnings_data
            
        # Fallback: Try old format for backwards compatibility
        try:
            page_data_string = [row for row in page_content.split(
                '\n') if row.startswith('root.App.main = ')][0][:-1]
            page_data_string = page_data_string.split('root.App.main = ', 1)[1]
            return json.loads(page_data_string)
        except (IndexError, json.JSONDecodeError):
            raise ValueError('Unable to parse earnings data from page')
    
    def _extract_earnings_from_sveltekit(self, page_content):
        """Extract earnings data from SvelteKit data-sveltekit-fetched tags."""
        # Find all data-sveltekit-fetched script tags with visualization URL
        pattern = r'<script type="application/json" data-sveltekit-fetched data-url="https://query[12]\.finance\.yahoo\.com/v1/finance/visualization[^"]*"[^>]*>({.*?})</script>'
        matches = re.findall(pattern, page_content, re.DOTALL)
        
        for match in matches:
            try:
                parsed = json.loads(match)
                if 'body' not in parsed:
                    continue
                body = json.loads(parsed['body'])
                if 'finance' not in body or 'result' not in body['finance']:
                    continue
                    
                for result in body['finance']['result']:
                    if 'documents' not in result:
                        continue
                    for doc in result['documents']:
                        # Look for SP_EARNINGS type which contains earnings data
                        if doc.get('entityIdType') == 'SP_EARNINGS':
                            return self._convert_visualization_to_dict(result, doc)
            except (json.JSONDecodeError, KeyError):
                continue
        
        return None
    
    def _convert_visualization_to_dict(self, result, doc):
        """Convert visualization API format to legacy format for compatibility."""
        columns = [col['id'] for col in doc.get('columns', [])]
        rows = doc.get('rows', [])
        
        # Convert rows to list of dictionaries
        earnings_rows = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i] if i < len(row) else None
            earnings_rows.append(row_dict)
        
        # Get total count from result metadata
        total = result.get('total', len(rows))
        
        # Return in legacy format for compatibility
        return {
            'context': {
                'dispatcher': {
                    'stores': {
                        'ScreenerCriteriaStore': {
                            'meta': {
                                'total': total
                            }
                        },
                        'ScreenerResultsStore': {
                            'results': {
                                'rows': earnings_rows
                            }
                        }
                    }
                }
            }
        }

    def get_next_earnings_date(self, symbol):
        """Gets the next earnings date of symbol
        Args:
            symbol: A ticker symbol
        Returns:
            Unix timestamp of the next earnings date
        Raises:
            Exception: When symbol is invalid or earnings date is not available
        """
        url = '{0}/{1}'.format(BASE_STOCK_URL, symbol)
        try:
            page_data_dict = self._get_data_dict(url)
            # Try new format first - check if QuoteSummaryStore data is available
            try:
                return page_data_dict['context']['dispatcher']['stores']['QuoteSummaryStore']['calendarEvents']['earnings']['earningsDate'][0]['raw']
            except (KeyError, TypeError, IndexError):
                pass
            
            # For new SvelteKit format, use get_earnings_of to find next date
            earnings = self.get_earnings_of(symbol)
            if earnings and len(earnings) > 0:
                # Find the earliest upcoming earnings date
                now = dt.now(timezone.utc).replace(tzinfo=None)
                for earning in sorted(earnings, key=lambda x: x.get('startdatetime', '')):
                    start_datetime_str = earning.get('startdatetime', '')
                    if start_datetime_str:
                        # Parse ISO format datetime
                        try:
                            # Handle various datetime formats
                            if 'T' in start_datetime_str:
                                # Remove timezone suffix for parsing
                                dt_str = start_datetime_str.split('.')[0] if '.' in start_datetime_str else start_datetime_str[:19]
                                earnings_dt = dt.strptime(dt_str, '%Y-%m-%dT%H:%M:%S')
                                if earnings_dt >= now:
                                    return int(earnings_dt.timestamp())
                        except ValueError:
                            continue
                raise Exception('No upcoming earnings date found')
        except (ValueError, KeyError, TypeError) as e:
            raise Exception('Invalid Symbol or Unavailable Earnings Date') from e
        except Exception as e:
            raise Exception('Invalid Symbol or Unavailable Earnings Date') from e

    def earnings_on(self, date, offset=0, count=1):
        """Gets earnings calendar data from Yahoo! on a specific date.
        Args:
            date: A datetime.date instance representing the date of earnings data to be fetched.
            offset: Position to fetch earnings data from.
            count: Total count of earnings on date.
        Returns:
            An array of earnings calendar data on date given. E.g.,
            [
                {
                    "ticker": "AMS.S",
                    "companyshortname": "Ams AG",
                    "startdatetime": "2017-04-23T20:00:00.000-04:00",
                    "startdatetimetype": "TAS",
                    "epsestimate": null,
                    "epsactual": null,
                    "epssurprisepct": null,
                    "gmtOffsetMilliSeconds": 72000000
                },
                ...
            ]
        Raises:
            TypeError: When date is not a datetime.date object.
        """
        if offset >= count:
            return []

        if not isinstance(date, datetime.date):
            raise TypeError(
                'Date should be a datetime.date object')
        date_str = date.strftime('%Y-%m-%d')
        logger.debug('Fetching earnings data for %s', date_str)
        dated_url = '{0}?day={1}&offset={2}&size={3}'.format(
            BASE_URL, date_str, offset, OFFSET_STEP)
        page_data_dict = self._get_data_dict(dated_url)
        stores_dict = page_data_dict['context']['dispatcher']['stores']
        earnings_count = stores_dict['ScreenerCriteriaStore']['meta']['total']

        # Recursively fetch more earnings on this date
        new_offset = offset + OFFSET_STEP
        more_earnings = self.earnings_on(date, new_offset, earnings_count)
        curr_offset_earnings = stores_dict['ScreenerResultsStore']['results']['rows']

        return curr_offset_earnings + more_earnings

    def earnings_between(self, from_date, to_date):
        """Gets earnings calendar data from Yahoo! in a date range.
        Args:
            from_date: A datetime.date instance representing the from-date (inclusive).
            to_date: A datetime.date instance representing the to-date (inclusive).
        Returns:
            An array of earnigs calendar data of date range. E.g.,
            [
                {
                    "ticker": "AMS.S",
                    "companyshortname": "Ams AG",
                    "startdatetime": "2017-04-23T20:00:00.000-04:00",
                    "startdatetimetype": "TAS",
                    "epsestimate": null,
                    "epsactual": null,
                    "epssurprisepct": null,
                    "gmtOffsetMilliSeconds": 72000000
                },
                ...
            ]
        Raises:
            ValueError: When from_date is after to_date.
            TypeError: When either from_date or to_date is not a datetime.date object.
        """
        if from_date > to_date:
            raise ValueError(
                'From-date should not be after to-date')
        if not (isinstance(from_date, datetime.date) and
                isinstance(to_date, datetime.date)):
            raise TypeError(
                'From-date and to-date should be datetime.date objects')
        earnings_data = []
        current_date = from_date
        delta = datetime.timedelta(days=1)
        while current_date <= to_date:
            earnings_data += self.earnings_on(current_date)
            current_date += delta
        return earnings_data

    def get_earnings_of(self, symbol):
        """Returns all the earnings dates of a symbol
        Args:
            symbol: A ticker symbol
        Returns:
            Array of all earnings dates with supplemental information
        Raises:
            Exception: When symbol is invalid or earnings date is not available
        """
        url = 'https://finance.yahoo.com/calendar/earnings?symbol={0}'.format(symbol)
        try: 
            page_data_dict = self._get_data_dict(url)
            return page_data_dict["context"]["dispatcher"]["stores"]["ScreenerResultsStore"]["results"]["rows"]
        except: 
            raise Exception('Invalid Symbol or Unavailable Earnings Date')

if __name__ == '__main__':  # pragma: no cover
    date_from = datetime.datetime.strptime(
        'Feb 1 2018  10:00AM', '%b %d %Y %I:%M%p')
    date_to = datetime.datetime.strptime(
        'Feb 4 2018  1:00PM', '%b %d %Y %I:%M%p')
    yec = YahooEarningsCalendar()
    print(yec.earnings_on(date_from))
    print(yec.earnings_between(date_from, date_to))
    # Returns the next earnings date of BOX in Unix timestamp
    print(yec.get_next_earnings_date('box'))
    # Returns a list of all available earnings of BOX
    print(yec.get_earnings_of('box'))
