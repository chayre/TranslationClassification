"""
find_dict_urls.py
=================
Tries several known URLs to find the FreeDict Polish-English dictionary.
"""

import urllib.request
import urllib.error

URLS_TO_TRY = [
    # GitHub raw content
    "https://raw.githubusercontent.com/freedict/fd-dictionaries/master/pol-eng/pol-eng.tei",
    "https://raw.githubusercontent.com/freedict/fd-dictionaries/main/pol-eng/pol-eng.tei",
    
    # FreeDict official
    "https://freedict.org/freedict-database/freedict-pol-eng-0.2.tei",
    "https://freedict.org/freedict-database/freedict-eng-pol-0.2.tei",
    
    # SourceForge
    "https://sourceforge.net/projects/freedict/files/Polish-English/",
    
    # Debian pool
    "http://ftp.debian.org/debian/pool/main/f/freedict/",
]

for url in URLS_TO_TRY:
    try:
        print(f"Trying: {url}")
        req = urllib.request.Request(url, method='HEAD')
        response = urllib.request.urlopen(req, timeout=10)
        print(f"  SUCCESS! Status: {response.status}")
    except urllib.error.HTTPError as e:
        print(f"  HTTP Error: {e.code}")
    except Exception as e:
        print(f"  Error: {e}")
    print()