import csv
import gzip
import hashlib
import json
import logging
import os
from itertools import chain
from pathlib import Path
from urllib.parse import urlparse

import requests

from mrfutils.exceptions import InvalidMRF

log = logging.getLogger('mrfutils')
log.setLevel(logging.INFO)


def prepend(value, iterator):
	"""Prepend a single value in front of an iterator
	>>>  prepend(1, [2, 3, 4])
	>>>  1 2 3 4
	"""
	return chain([value], iterator)


def peek(iterator):
	"""
	Usage:
	>>> next_, iter = peek(iter)
	allows you to peek at the next value of the iterator
	"""
	try: next_ = next(iterator)
	except StopIteration: return None, iterator
	return next_, prepend(next_, iterator)


class JSONOpen:
	"""
	Context manager for opening JSON(.gz) MRFs.
	Usage:
	>>> with JSONOpen('localfile.json') as f:
	or
	>>> with JSONOpen(some_json_url) as f:
	including both zipped and unzipped files.
	"""

	def __init__(self, filename):
		self.filename = filename
		self.f = None
		self.r = None
		self.is_remote = None
		self.buffer_size = 65536  # 64KB buffer

		parsed_url = urlparse(self.filename)
		self.suffix = ''.join(Path(parsed_url.path).suffixes)
		if not self.suffix:
			self.suffix = ''.join(Path(parsed_url.query).suffixes)

		if not (
			self.suffix.endswith('.json.gz') or
			self.suffix.endswith('.json')
		):
			raise InvalidMRF(f'Suffix not JSON: {self.filename=} {self.suffix=}')

		self.is_remote = parsed_url.scheme in ('http', 'https')

	def __enter__(self):
		if self.is_remote:
			self.s = requests.Session()
			# Configure session for better performance
			self.s.headers.update({'Accept-Encoding': 'gzip, deflate'})
			adapter = requests.adapters.HTTPAdapter(
				pool_connections=100,
				pool_maxsize=100,
				max_retries=3,
				pool_block=False
			)
			self.s.mount('http://', adapter)
			self.s.mount('https://', adapter)
			
			self.r = self.s.get(self.filename, stream=True)
			
			if self.suffix.endswith('.json.gz'):
				self.f = gzip.GzipFile(fileobj=self.r.raw)
			else:
				self.r.raw.decode_content = True
				self.f = self.r.raw
		else:
			if self.suffix.endswith('.json.gz'):
				self.f = gzip.open(self.filename, 'rb', buffering=self.buffer_size)
			else:
				self.f = open(self.filename, 'rb', buffering=self.buffer_size)

		log.info(f'Opened file: {self.filename}')
		return self.f

	def __exit__(self, exc_type, exc_val, exc_tb):
		if self.is_remote:
			self.s.close()
			self.r.close()
		self.f.close()


def import_csv_to_set(filename: str):
	"""Imports data as tuples from a given file."""
	items = set()

	# Use a larger buffer size for better performance
	with open(filename, 'r', buffering=65536) as f:
		# Skip header row
		next(f, None)
		
		reader = csv.reader(f)
		for row in reader:
			if not row:  # Skip empty rows
				continue
			row = [col.strip() for col in row if col.strip()]  # Skip empty columns
			if not row:  # Skip if all columns were empty
				continue
			
			if len(row) > 1:
				items.add(tuple(row))
			else:
				item = row[0]
				if item.isdigit():  # Only add if it's a valid number
					items.add(int(item))
				else:
					items.add(item)
		return items


def make_dir(out_dir):
	"""Create directory if it doesn't exist"""
	os.makedirs(out_dir, exist_ok=True)  # More efficient than checking then creating


def dicthasher(data: dict, n_bytes = 8) -> int:

	if not data:
		raise Exception("Hashed dictionary can't be empty")

	data = json.dumps(data, sort_keys=True).encode('utf-8')
	hash_s = hashlib.sha256(data).digest()[:n_bytes]
	hash_i = int.from_bytes(hash_s, 'little')

	return hash_i


def append_hash(item: dict, name: str) -> dict:

	hash_ = dicthasher(item)
	item[name] = hash_

	return item


def filename_hasher(filename: str) -> int:

	# retrieve/only/this_part_of_the_file.json(.gz)
	filename = Path(filename).stem.split('.')[0]
	file_row = {'filename': filename}
	filename_hash = dicthasher(file_row)

	return filename_hash


def validate_url(test_url: str) -> bool:
	# https://stackoverflow.com/a/38020041
	try:
		result = urlparse(test_url)
		return all([result.scheme, result.netloc])
	except:
		return False
