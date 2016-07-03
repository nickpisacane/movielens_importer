from os import path
import zipfile
import csv
import tempfile
import requests

MOVIE_LENS_URIS = {
    'small': 'http://files.grouplens.org/datasets/movielens/ml-latest-small.zip',
    'full': 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'
}

class Importer:

    def __init__(self, dataset='small', filename=None, normalize_imdb=False):
        self._filename = filename
        if not self._filename:
            if not dataset in MOVIE_LENS_URIS:
                raise Exception('movielens: Unknown data set ({}) options are: "small", "full"'.format(dataset))
            self._uri = MOVIE_LENS_URIS[dataset]
            self._temp_filename = path.join(tempfile.gettempdir(), '.movielens_importer_{}.zip'.format(dataset))

        self._normalize_imdb = normalize_imdb
        self._files = {}
        self._zip = None
        self._fp = None
        self._links = None

    def __del__(self):
        if self._zip: self._zip.close()
        if self._fp: self._fp.close()

    def _maybe_load_zip(self):
        if self._zip: return
        if self._filename:
            self._zip = zipfile.ZipFile(self._filename)
            return
        if path.isfile(self._temp_filename):
            print('Loading from temp file: {}.'.format(self._temp_filename))
            self._zip = zipfile.ZipFile(self._temp_filename)
            return
        r = requests.get(self._uri, stream=True)
        self._fp = open(self._temp_filename, 'w+')
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                self._fp.write(chunk)
        r.close()
        self._fp.flush()
        self._zip = zipfile.ZipFile(self._fp)

    def _read_file_as_dict_reader(self, filename):
        self._maybe_load_zip()
        filenames = filter(lambda fn: fn.endswith(filename), self._zip.namelist())
        if len(filenames) == 0:
            raise Exception('movielens: zip does not contain file: {}'.format(filename))
        if len(filenames) > 1:
            raise Exception('movielens: filename ({}) is ambiguous.'.format(filename))
        fn = filenames[0]
        return csv.DictReader(self._zip.read(fn).splitlines())

    def _get_links(self):
        if self._links:
            return self._links

        links = self.read_file('links.csv')
        self._links = {}
        for row in links:
            self._links[row['movieId']] = {'imdbId': row['imdbId'], 'tmdbId': row['tmdbId']}
        return self._links

    def read_file(self, filename):
        if filename in self._files:
            return self._files[filename]
        dict_reader = self._read_file_as_dict_reader(filename)
        is_normalizing = filename != 'links.csv' and \
            ('movieId' in dict_reader.fieldnames)
        self._files[filename] = []

        if is_normalizing:
            links = self._get_links()

        for row in dict_reader:
            if is_normalizing and row['movieId'] in links:
                link = links[row['movieId']]
                row['imdbId'] = link['imdbId'] if not self._normalize_imdb \
                    else 'tt{}'.format(link['imdbId'].zfill(7))
                row['tmdbId'] = link['tmdbId']
            self._files[filename].append(row)
        return self._files[filename]

    def filenames(self):
        self._maybe_load_zip()
        return filter(lambda fn: not not fn,
            map(lambda fn: path.basename(fn), self._zip.namelist())
        )

    def zip_filename(self):
        if not self._zip:
            raise Exception('movielens: no data loaded.')
        return self._filename if self._filename else self._temp_filename

if __name__ == '__main__':
    movielens = Importer(dataset='small', normalize_imdb=True)
    ratings = movielens.read_file('ratings.csv')
    print(ratings[0])
    print(movielens.zip_filename())
