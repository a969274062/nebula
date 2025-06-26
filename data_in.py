from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import numpy as np
import pandas as pd

data_df = pd.read_excel("/root/VscodeProject/PythonProject/nebula_data/论文数据_带正文_486_保密科学技术.xlsx", 
                      usecols=['论文标题', '期刊名称', '作者姓名', '单位', '摘要', '关键词', '发表时间', '专辑', '专题', '分类号', '下载量', '页数', '引用量'])

# 处理数据并插入到数据库
import re
# 预处理函数：处理字符串以便可以安全地在nGQL查询中使用
def preprocess_string(s):
    if pd.isna(s) or s == "无":
        return ""
    # 转为字符串并去除引号，防止SQL注入
    return str(s).replace("'", "''").strip()

# 处理页数字段，提取数字部分
def extract_page_number(page_str):
    if pd.isna(page_str):
        return "0"
    # 使用正则表达式提取数字部分
    match = re.search(r'\d+', str(page_str))
    if match:
        return match.group()
    return "0"

# 分割多作者
def split_authors(authors_str):
    if pd.isna(authors_str) or authors_str == "无":
        return []
    return [author.strip() for author in re.split('[；;，,、]', authors_str) if author.strip()]

# 分割关键词
def split_keywords(keywords_str):
    if pd.isna(keywords_str) or keywords_str == "无":
        return []
    return [kw.strip() for kw in re.split('[；;，,、]', keywords_str) if kw.strip()]

# 分割分类号
def split_classification(class_str):
    if pd.isna(class_str) or class_str == "无":
        return []
    return [cls.strip() for cls in re.split('[；;，,、]', class_str) if cls.strip()]

# 分割单位
def split_organizations(org_str):
    if pd.isna(org_str) or org_str == "无":
        return []
    return [org.strip() for org in re.split('[；;，,、]', org_str) if org.strip()]

# 分割专题
def split_topics(topic_str):
    if pd.isna(topic_str) or topic_str == "无":
        return []
    return [topic.strip() for topic in re.split('[；;，,、]', topic_str) if topic.strip()]

# 分割专辑
def split_albums(album_str):
    if pd.isna(album_str) or album_str == "无":
        return []
    return [album.strip() for album in re.split('[；;，,、]', album_str) if album.strip()]

config = Config() # 定义一个配置
config.max_connection_pool_size = 10 # 设置最大连接数
connection_pool = ConnectionPool() # 初始化连接池
# 如果给定的服务器是ok的，返回true，否则返回false
ok = connection_pool.init([('127.0.0.1', 9669)], config)

with connection_pool.session_context('root', 'nebula') as session:
    # 创建图空间test_paperdata
    try:
        # 检查图空间是否已存在
        check_space_result = session.execute('SHOW SPACES')
        existing_spaces = [record.values()[0].as_string() for record in check_space_result]
        
        if 'test_paperdata' not in existing_spaces:
            print("创建新的图空间: test_paperdata")
            # 创建图空间
            create_space_stmt = '''
            CREATE SPACE IF NOT EXISTS test_paperdata (
                vid_type = FIXED_STRING(256),
                partition_num = 10,
                replica_factor = 1
            )
            '''
            session.execute(create_space_stmt)
            
            print("等待图空间创建完成...")
            import time
            time.sleep(20)  # 等待图空间创建完成
            
            # 切换到新创建的图空间
            session.execute('USE test_paperdata')
            
            # 创建索引和Tags/Edges
            print("创建标签（Tags）和边类型（Edge Types）...")
            
            # 创建标签
            session.execute('CREATE TAG IF NOT EXISTS paper(title string, abstract string, release_time string, download_times int, page int, quote_times int)')
            session.execute('CREATE TAG IF NOT EXISTS journal(name string)')
            session.execute('CREATE TAG IF NOT EXISTS author(name string)')
            session.execute('CREATE TAG IF NOT EXISTS organization(name string)')
            session.execute('CREATE TAG IF NOT EXISTS key_word(name string)')
            session.execute('CREATE TAG IF NOT EXISTS classification_number(name string)')
            session.execute('CREATE TAG IF NOT EXISTS topic(name string)')
            session.execute('CREATE TAG IF NOT EXISTS album(name string)')
            session.execute('CREATE TAG IF NOT EXISTS key_technology(entity_name string, sensitive int)')
            session.execute('CREATE TAG IF NOT EXISTS institution(entity_name string, sensitive int)')
            session.execute('CREATE TAG IF NOT EXISTS application(entity_name string, sensitive int)')
            session.execute('CREATE TAG IF NOT EXISTS device_system(entity_name string, sensitive int)')
            
            # 创建边类型
            session.execute('CREATE EDGE IF NOT EXISTS which_journal()')
            session.execute('CREATE EDGE IF NOT EXISTS which_author()')
            session.execute('CREATE EDGE IF NOT EXISTS which_organization()')
            session.execute('CREATE EDGE IF NOT EXISTS which_key_word()')
            session.execute('CREATE EDGE IF NOT EXISTS which_classification_number()')
            session.execute('CREATE EDGE IF NOT EXISTS which_topic()')
            session.execute('CREATE EDGE IF NOT EXISTS which_album()')
            session.execute('CREATE EDGE IF NOT EXISTS related_to_paper()')
            
            # 创建索引
            print("创建索引...")
            session.execute('CREATE TAG INDEX IF NOT EXISTS i_paper ON paper()')
            print("等待索引构建完成...")
            time.sleep(20)  # 等待索引构建完成
            
            print("图空间 test_paperdata 创建成功！")
            result = session.execute('SHOW TAGS')
            print(result)
        else:
            print("图空间 test_paperdata 已存在，将直接使用。")
            session.execute('USE test_paperdata')
    except Exception as e:
        print(f"创建图空间时出错: {str(e)}")

    # 追踪已插入的顶点，避免重复
    inserted_vertices = {
        'journal': set(),
        'author': set(),
        'organization': set(),
        'key_word': set(),
        'classification': set(),
        'topic': set(),
        'album': set()
    }
    
    # 创建顶点ID映射和计数器
    vid_mapping = {}  # 存储实体名称到VID的映射
    counter = {
        'paper': 1,
        'journal': 1,
        'author': 1,
        'organization': 1,
        'key_word': 1,
        'classification': 1,
        'topic': 1,
        'album': 1,
        'application': 1,
        'device_system': 1,
        'institution': 1,
        'key_technology': 1
    }
    
    # 生成VID的函数
    def generate_vid(entity_type, entity_name):
        if entity_name in vid_mapping:
            return vid_mapping[entity_name]
        
        # 为该实体创建新的VID
        new_vid = f"{entity_type}{counter[entity_type]:02d}"
        vid_mapping[entity_name] = new_vid
        counter[entity_type] += 1
        return new_vid
    
    # 批量插入的语句列表
    nGQL_statements = []
    
    # 遍历每一行数据
    for index, row in data_df.iterrows():
        paper_title = preprocess_string(row['论文标题'])
        paper_abstract = preprocess_string(row['摘要'])
        paper_publish_time = preprocess_string(row['发表时间'])
        paper_downloads = preprocess_string(row['下载量'])
        paper_pages = extract_page_number(row['页数'])
        paper_citations = row['引用量']
        
        journal_name = preprocess_string(row['期刊名称'])
        
        # 生成论文顶点ID
        paper_vid = generate_vid('paper', paper_title)
        
        # 处理论文顶点
        nGQL_statements.append(f'''
        INSERT VERTEX IF NOT EXISTS paper(title, abstract, release_time, download_times, page, quote_times) 
        VALUES "{paper_vid}":('{paper_title}', '{paper_abstract}', '{paper_publish_time}', {int(paper_downloads)}, {int(paper_pages)}, {int(paper_citations)})
        ''')
        
        # 处理期刊顶点
        if journal_name and journal_name not in inserted_vertices['journal']:
            journal_vid = generate_vid('journal', journal_name)
            nGQL_statements.append(f'''
            INSERT VERTEX IF NOT EXISTS journal(name) VALUES "{journal_vid}":('{journal_name}')
            ''')
            inserted_vertices['journal'].add(journal_name)
        
        # 添加论文与期刊的关系
        if journal_name:
            journal_vid = vid_mapping[journal_name]
            nGQL_statements.append(f'''
            INSERT EDGE IF NOT EXISTS which_journal() VALUES "{paper_vid}"->"{journal_vid}":()
            ''')
        
        # 处理作者
        authors = split_authors(row['作者姓名'])
        for author in authors:
            if author and author not in inserted_vertices['author']:
                author_vid = generate_vid('author', author)
                nGQL_statements.append(f'''
                INSERT VERTEX IF NOT EXISTS author(name) VALUES "{author_vid}":('{author}')
                ''')
                inserted_vertices['author'].add(author)
            
            if author:
                author_vid = vid_mapping[author]
                nGQL_statements.append(f'''
                INSERT EDGE IF NOT EXISTS which_author() VALUES "{paper_vid}"->"{author_vid}":()
                ''')
        
        # 处理单位
        orgs = split_organizations(row['单位'])
        for org in orgs:
            if org and org not in inserted_vertices['organization']:
                org_vid = generate_vid('organization', org)
                nGQL_statements.append(f'''
                INSERT VERTEX IF NOT EXISTS organization(name) VALUES "{org_vid}":('{org}')
                ''')
                inserted_vertices['organization'].add(org)
            
            if org:
                org_vid = vid_mapping[org]
                nGQL_statements.append(f'''
                INSERT EDGE IF NOT EXISTS which_organization() VALUES "{paper_vid}"->"{org_vid}":()
                ''')
        
        # 处理关键词
        keywords = split_keywords(row['关键词'])
        for keyword in keywords:
            if keyword and keyword not in inserted_vertices['key_word']:
                keyword_vid = generate_vid('key_word', keyword)
                nGQL_statements.append(f'''
                INSERT VERTEX IF NOT EXISTS key_word(name) VALUES "{keyword_vid}":('{keyword}')
                ''')
                inserted_vertices['key_word'].add(keyword)
            
            if keyword:
                keyword_vid = vid_mapping[keyword]
                nGQL_statements.append(f'''
                INSERT EDGE IF NOT EXISTS which_key_word() VALUES "{paper_vid}"->"{keyword_vid}":()
                ''')
        
        # 处理分类号
        classifications = split_classification(row['分类号'])
        for cls in classifications:
            if cls and cls not in inserted_vertices['classification']:
                cls_vid = generate_vid('classification', cls)
                nGQL_statements.append(f'''
                INSERT VERTEX IF NOT EXISTS classification_number(name) VALUES "{cls_vid}":('{cls}')
                ''')
                inserted_vertices['classification'].add(cls)
            
            if cls:
                cls_vid = vid_mapping[cls]
                nGQL_statements.append(f'''
                INSERT EDGE IF NOT EXISTS which_classification_number() VALUES "{paper_vid}"->"{cls_vid}":()
                ''')
        
        # 处理专题
        topics = split_topics(row['专题'])
        for topic in topics:
            if topic and topic not in inserted_vertices['topic']:
                topic_vid = generate_vid('topic', topic)
                nGQL_statements.append(f'''
                INSERT VERTEX IF NOT EXISTS topic(name) VALUES "{topic_vid}":('{topic}')
                ''')
                inserted_vertices['topic'].add(topic)
            
            if topic:
                topic_vid = vid_mapping[topic]
                nGQL_statements.append(f'''
                INSERT EDGE IF NOT EXISTS which_topic() VALUES "{paper_vid}"->"{topic_vid}":()
                ''')
        
        # 处理专辑
        albums = split_albums(row['专辑'])
        for album in albums:
            if album and album not in inserted_vertices['album']:
                album_vid = generate_vid('album', album)
                nGQL_statements.append(f'''
                INSERT VERTEX IF NOT EXISTS album(name) VALUES "{album_vid}":('{album}')
                ''')
                inserted_vertices['album'].add(album)
            
            if album:
                album_vid = vid_mapping[album]
                nGQL_statements.append(f'''
                INSERT EDGE IF NOT EXISTS which_album() VALUES "{paper_vid}"->"{album_vid}":()
                ''')
    
    # 执行所有语句
    print(f"共计 {len(nGQL_statements)} 条语句等待执行")
    
    # 批量执行插入语句，每500条执行一次
    batch_size = 500
    for i in range(0, len(nGQL_statements), batch_size):
        batch = nGQL_statements[i:i+batch_size]
        for stmt in batch:
            try:
                result = session.execute(stmt)
                if not result.is_succeeded():
                    print(f"执行失败: {stmt}")
                    print(f"错误信息: {result.error_msg()}")
            except Exception as e:
                print(f"执行异常: {stmt}")
                print(f"异常信息: {str(e)}")
        print(f"已执行 {min(i+batch_size, len(nGQL_statements))} / {len(nGQL_statements)} 条语句")
    
    print("元数据导入完成")

# 关闭连接池
connection_pool.close()

config = Config() # 定义一个配置
config.max_connection_pool_size = 10 # 设置最大连接数
connection_pool = ConnectionPool() # 初始化连接池
# 如果给定的服务器是ok的，返回true，否则返回false
ok = connection_pool.init([('127.0.0.1', 9669)], config)
data_df = pd.read_excel("/root/VscodeProject/PythonProject/nebula_data/关键技术f.xlsx", 
                      usecols=['实体', '敏感属性', '关联论文'])


with connection_pool.session_context('root', 'nebula') as session:
    # 使用test_paperdata空间
    session.execute('USE test_paperdata')
    
    # 追踪已插入的顶点，避免重复
    inserted_vertices = {
        'entity': set(),
    }
        # 创建顶点ID映射和计数器
    vid_mapping = {}  # 存储实体名称到VID的映射
    counter = {
        'application': 1,
        'device_system': 1,
        'institution': 1,
        'key_technology': 1
    }
    # 批量插入的语句列表
    nGQL_statements = []
    
    # 遍历每一行数据
    for index, row in data_df.iterrows():
        entity_name = preprocess_string(row['实体'])
        sensitive = int(row['敏感属性']) if not pd.isna(row['敏感属性']) else 0
        related_papers_str = preprocess_string(row['关联论文'])
        
        # 处理关键技术实体顶点
        if entity_name and entity_name not in inserted_vertices['entity']:
            entity_vid = generate_vid('key_technology', entity_name)
# 修正语法：注意引号的正确使用方式
            nGQL_statements.append(f'''
            INSERT VERTEX IF NOT EXISTS key_technology(entity_name, sensitive) 
            VALUES "{entity_vid}":("{entity_name}", {sensitive})
            ''')
            inserted_vertices['entity'].add(entity_name)
        
        # 处理关联论文 - 分割可能包含多个论文的字符串
        if related_papers_str:
            # 清理格式：移除多余的引号、方括号和其他干扰字符
            clean_papers_str = related_papers_str.replace('[', '').replace(']', '').replace('"', '').replace("'", "")
            
            # 使用方括号内的任意标点符号分割论文标题
            related_papers = [paper.strip() for paper in re.split('[,]', clean_papers_str) if paper.strip()]
            
            not_found_papers = []
            for paper_title in related_papers:
                
                # 检查论文是否存在于图数据库中
                check_result = session.execute(f'LOOKUP ON paper WHERE paper.title == "{paper_title}" YIELD id(VERTEX)')

                if check_result.is_succeeded() and check_result.rows():
                    # 从查询结果中正确提取论文VID
                    paper_vid = check_result.column_values('id(VERTEX)')[0]  # 使用column_values获取VID
                    # 论文存在，添加关联关系
                    nGQL_statements.append(f'''
                    INSERT EDGE IF NOT EXISTS related_to_paper() VALUES "{entity_vid}"->{paper_vid}:()
                    ''')
                else:
                    # 论文不存在，记录下来
                    not_found_papers.append(paper_title)
            
            # 如果有未找到的论文，打印出来并保存到文件
            if not_found_papers:
                print(f"实体 '{entity_name}' 关联的以下论文未找到: {not_found_papers}")
                
                # 将未找到的论文信息保存到文件
                with open('/root/VscodeProject/PythonProject/nebula_data/not_found_papers.txt', 'a', encoding='utf-8') as f:
                    f.write(f"实体: {entity_name}\n")
                    for paper in not_found_papers:
                        f.write(f"  - {paper}\n")
                    f.write("\n")
    
    # 执行所有语句
    print(f"共计 {len(nGQL_statements)} 条语句等待执行")
    
    # 批量执行插入语句，每500条执行一次
    batch_size = 500
    for i in range(0, len(nGQL_statements), batch_size):
        batch = nGQL_statements[i:i+batch_size]
        for stmt in batch:
            try:
                result = session.execute(stmt)
                if not result.is_succeeded():
                    print(f"执行失败: {stmt}")
                    print(f"错误信息: {result.error_msg()}")
            except Exception as e:
                print(f"执行异常: {stmt}")
                print(f"异常信息: {str(e)}")
        print(f"已执行 {min(i+batch_size, len(nGQL_statements))} / {len(nGQL_statements)} 条语句")
    
    print("关键技术数据导入完成")

# 关闭连接池
connection_pool.close()

config = Config() # 定义一个配置
config.max_connection_pool_size = 10 # 设置最大连接数
connection_pool = ConnectionPool() # 初始化连接池
# 如果给定的服务器是ok的，返回true，否则返回false
ok = connection_pool.init([('127.0.0.1', 9669)], config)
data_df = pd.read_excel("/root/VscodeProject/PythonProject/nebula_data/组织与机构f.xlsx", 
                      usecols=['实体', '敏感属性', '关联论文'])


with connection_pool.session_context('root', 'nebula') as session:
    # 使用paperdata空间
    session.execute('USE test_paperdata')
    
    # 追踪已插入的顶点，避免重复
    inserted_vertices = {
        'entity': set(),
    }
     # 创建顶点ID映射和计数器
    vid_mapping = {}  # 存储实体名称到VID的映射
    counter = {
        'application': 1,
        'device_system': 1,
        'institution': 1,
        'key_technology': 1
    }
    # 批量插入的语句列表
    nGQL_statements = []
    
    # 遍历每一行数据
    for index, row in data_df.iterrows():
        entity_name = preprocess_string(row['实体'])
        sensitive = int(row['敏感属性']) if not pd.isna(row['敏感属性']) else 0
        related_papers_str = preprocess_string(row['关联论文'])
        
        # 处理关键技术实体顶点
        if entity_name and entity_name not in inserted_vertices['entity']:
            entity_vid = generate_vid('institution', entity_name)
# 修正语法：注意引号的正确使用方式
            nGQL_statements.append(f'''
            INSERT VERTEX IF NOT EXISTS institution(entity_name, sensitive) 
            VALUES "{entity_vid}":("{entity_name}", {sensitive})
            ''')
            inserted_vertices['entity'].add(entity_name)
        
                # 处理关联论文 - 分割可能包含多个论文的字符串
        if related_papers_str:
            # 清理格式：移除多余的引号、方括号和其他干扰字符
            clean_papers_str = related_papers_str.replace('[', '').replace(']', '').replace('"', '').replace("'", "")
            
            # 使用方括号内的任意标点符号分割论文标题
            related_papers = [paper.strip() for paper in re.split('[,]', clean_papers_str) if paper.strip()]
            
            not_found_papers = []
            for paper_title in related_papers:
                
                # 检查论文是否存在于图数据库中
                check_result = session.execute(f'LOOKUP ON paper WHERE paper.title == "{paper_title}" YIELD id(VERTEX)')

                if check_result.is_succeeded() and check_result.rows():
                    # 从查询结果中正确提取论文VID
                    paper_vid = check_result.column_values('id(VERTEX)')[0]  # 使用column_values获取VID
                    # 论文存在，添加关联关系
                    nGQL_statements.append(f'''
                    INSERT EDGE IF NOT EXISTS related_to_paper() VALUES "{entity_vid}"->{paper_vid}:()
                    ''')
                else:
                    # 论文不存在，记录下来
                    not_found_papers.append(paper_title)
            
            # 如果有未找到的论文，打印出来并保存到文件
            if not_found_papers:
                print(f"实体 '{entity_name}' 关联的以下论文未找到: {not_found_papers}")
                
                # 将未找到的论文信息保存到文件
                with open('/root/VscodeProject/PythonProject/nebula_data/not_found_papers.txt', 'a', encoding='utf-8') as f:
                    f.write(f"实体: {entity_name}\n")
                    for paper in not_found_papers:
                        f.write(f"  - {paper}\n")
                    f.write("\n")
    
    # 执行所有语句
    print(f"共计 {len(nGQL_statements)} 条语句等待执行")
    
    # 批量执行插入语句，每500条执行一次
    batch_size = 500
    for i in range(0, len(nGQL_statements), batch_size):
        batch = nGQL_statements[i:i+batch_size]
        for stmt in batch:
            try:
                result = session.execute(stmt)
                if not result.is_succeeded():
                    print(f"执行失败: {stmt}")
                    print(f"错误信息: {result.error_msg()}")
            except Exception as e:
                print(f"执行异常: {stmt}")
                print(f"异常信息: {str(e)}")
        print(f"已执行 {min(i+batch_size, len(nGQL_statements))} / {len(nGQL_statements)} 条语句")
    
    print("组织与机构数据导入完成")

# 关闭连接池
connection_pool.close()

config = Config() # 定义一个配置
config.max_connection_pool_size = 10 # 设置最大连接数
connection_pool = ConnectionPool() # 初始化连接池
# 如果给定的服务器是ok的，返回true，否则返回false
ok = connection_pool.init([('127.0.0.1', 9669)], config)
data_df = pd.read_excel("/root/VscodeProject/PythonProject/nebula_data/应用场景与领域f.xlsx", 
                      usecols=['实体', '敏感属性', '关联论文'])


with connection_pool.session_context('root', 'nebula') as session:
    # 使用paperdata空间
    session.execute('USE test_paperdata')
    
    # 追踪已插入的顶点，避免重复
    inserted_vertices = {
        'entity': set(),
    }
     # 创建顶点ID映射和计数器
    vid_mapping = {}  # 存储实体名称到VID的映射
    counter = {
        'application': 1,
        'device_system': 1,
        'institution': 1,
        'key_technology': 1
    }
    # 批量插入的语句列表
    nGQL_statements = []
    
    # 遍历每一行数据
    for index, row in data_df.iterrows():
        entity_name = preprocess_string(row['实体'])
        sensitive = int(row['敏感属性']) if not pd.isna(row['敏感属性']) else 0
        related_papers_str = preprocess_string(row['关联论文'])
        
        # 处理关键技术实体顶点
        if entity_name and entity_name not in inserted_vertices['entity']:
            entity_vid = generate_vid('application', entity_name)
# 修正语法：注意引号的正确使用方式
            nGQL_statements.append(f'''
            INSERT VERTEX IF NOT EXISTS application(entity_name, sensitive) 
            VALUES "{entity_vid}":("{entity_name}", {sensitive})
            ''')
            inserted_vertices['entity'].add(entity_name)
        
                # 处理关联论文 - 分割可能包含多个论文的字符串
        if related_papers_str:
            # 清理格式：移除多余的引号、方括号和其他干扰字符
            clean_papers_str = related_papers_str.replace('[', '').replace(']', '').replace('"', '').replace("'", "")
            
            # 使用方括号内的任意标点符号分割论文标题
            related_papers = [paper.strip() for paper in re.split('[,]', clean_papers_str) if paper.strip()]
            
            not_found_papers = []
            for paper_title in related_papers:
                
                # 检查论文是否存在于图数据库中
                check_result = session.execute(f'LOOKUP ON paper WHERE paper.title == "{paper_title}" YIELD id(VERTEX)')

                if check_result.is_succeeded() and check_result.rows():
                    # 从查询结果中正确提取论文VID
                    paper_vid = check_result.column_values('id(VERTEX)')[0]  # 使用column_values获取VID
                    # 论文存在，添加关联关系
                    nGQL_statements.append(f'''
                    INSERT EDGE IF NOT EXISTS related_to_paper() VALUES "{entity_vid}"->{paper_vid}:()
                    ''')
                else:
                    # 论文不存在，记录下来
                    not_found_papers.append(paper_title)
            
            # 如果有未找到的论文，打印出来并保存到文件
            if not_found_papers:
                print(f"实体 '{entity_name}' 关联的以下论文未找到: {not_found_papers}")
                
                # 将未找到的论文信息保存到文件
                with open('/root/VscodeProject/PythonProject/nebula_data/not_found_papers.txt', 'a', encoding='utf-8') as f:
                    f.write(f"实体: {entity_name}\n")
                    for paper in not_found_papers:
                        f.write(f"  - {paper}\n")
                    f.write("\n")
    
    # 执行所有语句
    print(f"共计 {len(nGQL_statements)} 条语句等待执行")
    
    # 批量执行插入语句，每500条执行一次
    batch_size = 500
    for i in range(0, len(nGQL_statements), batch_size):
        batch = nGQL_statements[i:i+batch_size]
        for stmt in batch:
            try:
                result = session.execute(stmt)
                if not result.is_succeeded():
                    print(f"执行失败: {stmt}")
                    print(f"错误信息: {result.error_msg()}")
            except Exception as e:
                print(f"执行异常: {stmt}")
                print(f"异常信息: {str(e)}")
        print(f"已执行 {min(i+batch_size, len(nGQL_statements))} / {len(nGQL_statements)} 条语句")
    
    print("应用场景与领域数据导入完成")

# 关闭连接池
connection_pool.close()

config = Config() # 定义一个配置
config.max_connection_pool_size = 10 # 设置最大连接数
connection_pool = ConnectionPool() # 初始化连接池
# 如果给定的服务器是ok的，返回true，否则返回false
ok = connection_pool.init([('127.0.0.1', 9669)], config)
data_df = pd.read_excel("/root/VscodeProject/PythonProject/nebula_data/设备与系统f.xlsx", 
                      usecols=['实体', '敏感属性', '关联论文'])


with connection_pool.session_context('root', 'nebula') as session:
    # 使用paperdata空间
    session.execute('USE test_paperdata')
    
    # 追踪已插入的顶点，避免重复
    inserted_vertices = {
        'entity': set(),
    }
     # 创建顶点ID映射和计数器
    vid_mapping = {}  # 存储实体名称到VID的映射
    counter = {
        'application': 1,
        'device_system': 1,
        'institution': 1,
        'key_technology': 1
    }
    # 批量插入的语句列表
    nGQL_statements = []
    
    # 遍历每一行数据
    for index, row in data_df.iterrows():
        entity_name = preprocess_string(row['实体'])
        sensitive = int(row['敏感属性']) if not pd.isna(row['敏感属性']) else 0
        related_papers_str = preprocess_string(row['关联论文'])
        
        # 处理关键技术实体顶点
        if entity_name and entity_name not in inserted_vertices['entity']:
            entity_vid = generate_vid('device_system', entity_name)
# 修正语法：注意引号的正确使用方式
            nGQL_statements.append(f'''
            INSERT VERTEX IF NOT EXISTS device_system(entity_name, sensitive) 
            VALUES "{entity_vid}":("{entity_name}", {sensitive})
            ''')
            inserted_vertices['entity'].add(entity_name)
        
                # 处理关联论文 - 分割可能包含多个论文的字符串
        if related_papers_str:
            # 清理格式：移除多余的引号、方括号和其他干扰字符
            clean_papers_str = related_papers_str.replace('[', '').replace(']', '').replace('"', '').replace("'", "")
            
            # 使用方括号内的任意标点符号分割论文标题
            related_papers = [paper.strip() for paper in re.split('[,]', clean_papers_str) if paper.strip()]
            
            not_found_papers = []
            for paper_title in related_papers:
                
                # 检查论文是否存在于图数据库中
                check_result = session.execute(f'LOOKUP ON paper WHERE paper.title == "{paper_title}" YIELD id(VERTEX)')

                if check_result.is_succeeded() and check_result.rows():
                    # 从查询结果中正确提取论文VID
                    paper_vid = check_result.column_values('id(VERTEX)')[0]  # 使用column_values获取VID
                    # 论文存在，添加关联关系
                    nGQL_statements.append(f'''
                    INSERT EDGE IF NOT EXISTS related_to_paper() VALUES "{entity_vid}"->{paper_vid}:()
                    ''')
                else:
                    # 论文不存在，记录下来
                    not_found_papers.append(paper_title)
            
            # 如果有未找到的论文，打印出来并保存到文件
            if not_found_papers:
                print(f"实体 '{entity_name}' 关联的以下论文未找到: {not_found_papers}")
                
                # 将未找到的论文信息保存到文件
                with open('/root/VscodeProject/PythonProject/nebula_data/not_found_papers.txt', 'a', encoding='utf-8') as f:
                    f.write(f"实体: {entity_name}\n")
                    for paper in not_found_papers:
                        f.write(f"  - {paper}\n")
                    f.write("\n")
    
    # 执行所有语句
    print(f"共计 {len(nGQL_statements)} 条语句等待执行")
    
    # 批量执行插入语句，每500条执行一次
    batch_size = 500
    for i in range(0, len(nGQL_statements), batch_size):
        batch = nGQL_statements[i:i+batch_size]
        for stmt in batch:
            try:
                result = session.execute(stmt)
                if not result.is_succeeded():
                    print(f"执行失败: {stmt}")
                    print(f"错误信息: {result.error_msg()}")
            except Exception as e:
                print(f"执行异常: {stmt}")
                print(f"异常信息: {str(e)}")
        print(f"已执行 {min(i+batch_size, len(nGQL_statements))} / {len(nGQL_statements)} 条语句")
    
    print("设备与系统数据导入完成")

# 关闭连接池
connection_pool.close()