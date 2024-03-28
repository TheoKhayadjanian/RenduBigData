# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class CountTagsByUser(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags_by_user,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_tags_by_user(self, _, line):
        try:
            user_id, movie_id, tag, timestamp = line.split(',')
            yield user_id, 1  
        except ValueError:
            pass
        except Exception as e:
            sys.stderr.write("Erreur inattendue: %s\n" % e)

    def reducer_count_tags(self, user_id, counts):
        yield user_id, sum(counts)

if __name__ == '__main__':
    CountTagsByUser.run()
