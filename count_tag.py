from mrjob.job import MRJob
from mrjob.step import MRStep

class CompteurTags(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags,
                   reducer=self.reducer_count_tags)
        ]

    def Mapping(self, _, line):
        try:
            user_id, movie_id, tag, timestamp = line.split(',')
            yield movie_id, 1
        except Exception:
            pass

    def Reduce(self, movie_id, counts):
        yield movie_id, sum(counts)

if __name__ == '__main__':
    CompteurTags.run()