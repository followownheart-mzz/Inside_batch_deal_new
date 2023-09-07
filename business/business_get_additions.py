from business.business_theme_deal.code_mode.main_codemode import MainCodeMode
from business.business_theme_deal.relation import Relation
from public.get_program_info import *


class GetNewAdditions:
    def __init__(self, program_id, df, number_of_times):
        self.program_id = program_id
        self.df = df
        self.number_of_times = number_of_times
        rest = get_info(self.program_id)
        self.from_type = rest[7]
        self.process_mode = rest[6]
        self.to_theme_id = rest[5]

    def add_column(self, df):
        if df is not None:
            df['businessCreateTime'] = datetime.datetime.now()
            df['businessUpdateTime'] = datetime.datetime.now()
            df['businessBatchNumber'] = self.number_of_times
        return df

    # 取出新数据-基础主题
    def get_data(self):
        if self.from_type == '基础主题':
            if self.process_mode == '直接对应':
                pass
            elif self.process_mode == '关联计算':
                self.df = Relation(self.df, self.program_id).main()
            else:
                self.df = MainCodeMode(self.df, self.program_id, '基础主题').main()
        else:
            if self.process_mode == '直接对应':
                pass
            elif self.process_mode == '关联计算':
                self.df = Relation(self.df, self.program_id).main()
            else:
                self.df = MainCodeMode(self.df, self.program_id, '业务主题').main()

        if self.df is not None:
            if len(self.df) > 0:
                last_batch_volume = len(self.df)
            else:
                last_batch_volume = 0
        else:
            last_batch_volume = 0

        if last_batch_volume > 0:
            max_create_time = self.df['createTime'].max()
            min_create_time = self.df['createTime'].min()
        else:
            max_create_time = None
            min_create_time = None

        self.df = self.add_column(self.df)
        return self.df, max_create_time, min_create_time, last_batch_volume
