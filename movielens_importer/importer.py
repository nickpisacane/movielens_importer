from os import path
import zipfile
import csv
import tempfile
import urllib

MOVIE_LENS_URIS = {
    'small': 'http://files.grouplens.org/datasets/movielens/ml-latest-small.zip',
    'full': 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'
}
FILE_FORMAT = 'movielens_importer_{}.zip'

class Importer:

    def __init__(self, dataset='small', filename=None, parse=True, normalize_imdb=False):
        if not dataset in MOVIE_LENS_URIS:
            raise ValueError('movielens: Unknown dataset.')

        self._filename = (filename or
            path.join(tempfile.gettempdir(), FILE_FORMAT.format(dataset)))
        self._uri = MOVIE_LENS_URIS[dataset]
        self._parse = parse
        self._normalize_imdb = normalize_imdb
        self._files = {}
        self._zip = None
        self._links = None

    def __del__(self):
        if self._zip: self._zip.close()

    def _maybe_load_zip(self):
        if self._zip: return
        if path.isfile(self._filename):
            self._zip = zipfile.ZipFile(self._filename)
        else:
            urllib.urlretrieve(self._uri, self._filename)
            self._zip = zipfile.ZipFile(self._filename)

    def _read_file(self, filename):
        self._maybe_load_zip()
        filenames = filter(lambda f: f.endswith(filename), self._zip.namelist())
        if len(filenames) == 0:
            raise IOError('movielens: No such file.')
        if len(filenames) > 1:
            raise ValueError('movielens: Filename is ambiguous.'.format(filename))
        return self._zip.read(filenames[0])

    def _get_links(self):
        if not self._links:
            data = self._read_file('links.csv')
            links_dict = csv.DictReader(data.splitlines())
            self._links = dict([
                (r['movieId'], { 'imdbId': r['imdbId'], 'tmdbId': r['tmdbId'] })
                for r in links_dict
            ])
        return self._links

    def read_file(self, filename):
        self._maybe_load_zip()
        if not filename in self._files:
            data = self._read_file(filename)
            if not self._parse:
                self._files[filename] = data
            else:
                dict_reader = csv.DictReader(data.splitlines())
                links = self._get_links()
                f = self._files[filename] = []
                for row in dict_reader:
                    link = links[row['movieId']]
                    row['imdbId'] = (link['imdbId'] if not self._normalize_imdb
                        else 'tt{}'.format(link['imdbId'].zfill(7)))
                    row['tmdbId'] = link['tmdbId']
                    f.append(row)
        return self._files[filename]

    def filenames(self):
        self._maybe_load_zip()
        return filter(lambda fn: not not fn,
            map(lambda fn: path.basename(fn), self._zip.namelist())
        )

    def zip_filename(self):
        if not self._zip:
            raise Exception('movielens: No data loaded.')
        return self._filename if self._filename else self._temp_filename

    def download(self):
        self._maybe_load_zip()

if __name__ == '__main__':
    movielens = Importer(dataset='full', normalize_imdb=True)
    links = movielens._get_links()
    print(links['1'])
