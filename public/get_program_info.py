# coding: utf-8
from public.database_conn import *


def get_info(program_id):
    sql = """ 
        select 
        b.id,
        b.name,
        c.name from_theme_name,
        a.name to_theme_name,
        b.from_theme_id,
        b.to_theme_id,
        b.process_mode,
        b.from_type
        from ana_theme a
        left join ana_program b
        on a.id=b.to_theme_id
        left join acq_theme c
        on b.from_theme_id=c.id
        where a.if_leaf='是' 
        and b.id=%s
        """
    conn_inside, cursor_inside = conn_inside_mysql()
    cursor_inside.execute(sql, (program_id,))
    # rest0 返回元组:（programId,programName,self.fromThemeName,to_theme_name,from_theme_id,to_theme_id,process_mode,from_type）
    rest0 = cursor_inside.fetchone()
    close_mysql(conn_inside, cursor_inside)

    program_id = rest0[0]
    program_name = rest0[1]
    from_theme_name = rest0[2]
    to_theme_name = rest0[3]
    from_theme_id = rest0[4]
    to_theme_id = rest0[5]
    process_mode = rest0[6]
    from_type = rest0[7]
    task_name = program_name
    return (
        program_id,
        program_name,
        from_theme_name,
        to_theme_name,
        from_theme_id,
        to_theme_id,
        process_mode,
        from_type,
        task_name,
    )
