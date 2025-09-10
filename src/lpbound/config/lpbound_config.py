class LpBoundConfig:

    def __init__(
        self,
        benchmark_name: str = "joblight",
        num_mcvs: int = 5000,
        num_buckets: int = 128,
        p_max: int = 10,
        include_l0: bool = True,
        include_l_inf: bool = True,
        enable_groupby: bool = False,
    ):

        self.benchmark_name: str = benchmark_name
        self.num_mcvs: int = num_mcvs
        self.num_buckets: int = num_buckets
        self.p_min: int = 1
        self.p_max: int = p_max
        self.include_l0: bool = include_l0
        self.include_l_inf: bool = include_l_inf
        self.enable_groupby: bool = enable_groupby
