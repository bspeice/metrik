from luigi.target import Target


class NoOpTarget(Target):
    def exists(self):
        return True