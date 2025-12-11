#!/usr/bin/env python3
"""
提取TTL文件的指定行范围数据（从第m行到第n行）
"""

def totail_lines(input_file):
    """
    统计TTL文件的行数
    """
    with open(input_file, 'r', encoding='utf-8') as infile:
        total_lines = sum(1 for _ in infile)
        print(f"TTL文件总行数: {total_lines}")

def extract_ttl_lines(input_file, output_file, start_line=1, end_line=5000000):
    """
    从输入的TTL文件中提取指定行范围的数据到输出文件
    
    Args:
        input_file: 输入文件路径
        output_file: 输出文件路径
        start_line: 起始行号（包含，默认为1）
        end_line: 结束行号（包含，默认为5000000）
    """
    print(f"开始从 {input_file} 提取第 {start_line} 行到第 {end_line} 行数据...")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            for i, line in enumerate(infile, 1):
                # 如果当前行号小于起始行，跳过
                if i < start_line:
                    continue
                # 如果当前行号大于结束行，停止处理
                elif i > end_line:
                    break
                else:
                    outfile.write(line)
                
                # 每10万行显示一次进度（只在处理范围内显示）
                if i >= start_line and (i - start_line + 1) % 100000 == 0:
                    print(f"已处理 {i - start_line + 1} 行（当前文件行号: {i}）...")
        
        extracted_lines = end_line - start_line + 1
        print(f"完成! 已将第 {start_line} 行到第 {end_line} 行数据（共 {extracted_lines} 行）写入到 {output_file}")
        
    except FileNotFoundError:
        print(f"错误: 找不到文件 {input_file}")
    except Exception as e:
        print(f"发生错误: {e}")

if __name__ == "__main__":
    input_file = "/Users/yangxk/data/mappingbased-objects_lang=en.ttl"
    output_file = "/Users/yangxk/data/limit_mappingbased-objects_lang=en.ttl"
    
    # 示例：提取第1000001行到第4000000行
    # extract_ttl_lines(input_file, output_file, start_line=1000001, end_line=4000000)
    # totail_lines(input_file)
    
    input_file = "/Users/yangxk/data/links_graph=wikidata-dbpedia-org_partition=owl-sameAs_cleaned.ttl"
    output_file = "/Users/yangxk/data/35M_40M_limit_links_graph=wikidata-dbpedia-org_partition=owl-sameAs.ttl"
    # totail_lines(input_file)
    
    # 示例：提取前1亿行（等同于原来的用法）
    # extract_ttl_lines(input_file, output_file, start_line=1, end_line=100000000)
    
    # 示例：提取第50000001行到第100000000行
    extract_ttl_lines(input_file, output_file, start_line=35000000, end_line=40000000)
