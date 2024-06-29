import pickle
import multiprocessing
from python_structured import *
from sqlang_structured import *


def multipro_python_query(data_list):
    # 对于数据列表中的每一项，调用python_query_parse进行解析
    return [python_query_parse(line) for line in data_list]

def multipro_python_code(data_list):
    # 对于数据列表中的每一项，调用python_code_parse进行解析
    return [python_code_parse(line) for line in data_list]

def multipro_python_context(data_list):
    result = []
    for line in data_list:
        # 遇到特定标记时直接添加到结果中
        if line == '-10000':
            result.append(['-10000'])
        # 其他情况调用python_context_parse解析上下文信息
        else:
            result.append(python_context_parse(line))
    return result

def multipro_sqlang_query(data_list):
    # 对于数据列表中的每一项，调用sqlang_query_parse进行解析
    return [sqlang_query_parse(line) for line in data_list]

def multipro_sqlang_code(data_list):
    # 对于数据列表中的每一项，调用sqlang_code_parse进行解析
    return [sqlang_code_parse(line) for line in data_list]

def multipro_sqlang_context(data_list):
    result = []
    for line in data_list:
        # 遇到特定标记时直接添加到结果中
        if line == '-10000':
            result.append(['-10000'])
        # 其他情况调用sqlang_context_parse解析上下文信息
        else:
            result.append(sqlang_context_parse(line))
    return result

# 并行解析数据，使用分割好的数据列表和指定的函数进行并行处理
def parse(data_list, split_num, context_func, query_func, code_func):
    pool = multiprocessing.Pool()
    # 将原始数据列表分割为多个小块，以便进行并行处理
    split_list = [data_list[i:i + split_num] for i in range(0, len(data_list), split_num)]
    # 并行处理上下文数据
    results = pool.map(context_func, split_list)
    context_data = [item for sublist in results for item in sublist]
    print(f'context条数：{len(context_data)}')
    # 并行处理查询数据
    results = pool.map(query_func, split_list)
    query_data = [item for sublist in results for item in sublist]
    print(f'query条数：{len(query_data)}')
    # 并行处理代码数据
    results = pool.map(code_func, split_list)
    code_data = [item for sublist in results for item in sublist]
    print(f'code条数：{len(code_data)}')
    # 关闭进程池
    pool.close()
    pool.join()

    return context_data, query_data, code_data

# 主函数，用于处理并保存指定语言类型数据集
def main(lang_type, split_num, source_path, save_path, context_func, query_func, code_func):
    # 加载原始数据集
    with open(source_path, 'rb') as f:
        corpus_lis = pickle.load(f)
    # 使用并行处理解析数据
    context_data, query_data, code_data = parse(corpus_lis, split_num, context_func, query_func, code_func)
    # 提取question ID
    qids = [item[0] for item in corpus_lis]
    # 组合数据
    total_data = [[qids[i], context_data[i], code_data[i], query_data[i]] for i in range(len(qids))]

    with open(save_path, 'wb') as f:
        pickle.dump(total_data, f)

if __name__ == '__main__':
    staqc_python_path = '.ulabel_data/python_staqc_qid2index_blocks_unlabeled.txt'
    staqc_python_save = '../hnn_process/ulabel_data/staqc/python_staqc_unlabled_data.pkl'

    staqc_sql_path = './ulabel_data/sql_staqc_qid2index_blocks_unlabeled.txt'
    staqc_sql_save = './ulabel_data/staqc/sql_staqc_unlabled_data.pkl'

    main(python_type, split_num, staqc_python_path, staqc_python_save, multipro_python_context, multipro_python_query, multipro_python_code)
    main(sqlang_type, split_num, staqc_sql_path, staqc_sql_save, multipro_sqlang_context, multipro_sqlang_query, multipro_sqlang_code)

    large_python_path = './ulabel_data/large_corpus/multiple/python_large_multiple.pickle'
    large_python_save = '../hnn_process/ulabel_data/large_corpus/multiple/python_large_multiple_unlable.pkl'

    large_sql_path = './ulabel_data/large_corpus/multiple/sql_large_multiple.pickle'
    large_sql_save = './ulabel_data/large_corpus/multiple/sql_large_multiple_unlable.pkl'

    main(python_type, split_num, large_python_path, large_python_save, multipro_python_context, multipro_python_query, multipro_python_code)
    main(sqlang_type, split_num, large_sql_path, large_sql_save, multipro_sqlang_context, multipro_sqlang_query, multipro_sqlang_code)
