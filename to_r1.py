import json
import argparse
import math  # 用于检查NaN

def is_nan(value):
    """检查值是否为NaN/None/null"""
    if value is None:
        return True
    try:
        return math.isnan(float(value))
    except (ValueError, TypeError):
        return False

def convert_json_format(input_file, output_file):
    # 从文件读取原始JSON数据
    with open(input_file, 'r', encoding='utf-8') as f:
        original_data = json.load(f)
    
    # 转换数据格式并过滤无效数据
    converted_data = []
    skipped_count = 0
    
    for item in original_data:
        # 检查关键字段是否存在且有效
        if ('Question' not in item or 'Complex_CoT' not in item or 'Response' not in item):
            skipped_count += 1
            continue
            
        question = item["Question"]
        response = item["Response"]
        
        # 过滤NaN/None/null值
        if is_nan(question) or is_nan(response):
            skipped_count += 1
            continue
        
        # 构建新条目
        new_item = {
            "instruction": """以下是描述任务的指令，以及提供更多上下文的输入。
                        请写出恰当完成该请求的回答。
                        在回答之前，请仔细思考问题，并创建一个逐步的思维链，以确保回答合乎逻辑且准确。

                        ### Instruction:
                        你是一个机器失效分析的专家，擅长逻辑严谨的推理。
                        请回答以下医学问题。

                        """,
            "input": "### Question:\n" + str(question).strip() + '\n',  # 确保是字符串并去除首尾空格
            "output": "### Response:\n<think>" + str(item["Complex_CoT"]).strip() + "</think>\n" + str(item["Complex_CoT"]).strip(),
        }
        converted_data.append(new_item)
    
    # 将转换后的数据写入新文件
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(converted_data, f, ensure_ascii=False, indent=2)
    
    print(f"转换完成: 共处理 {len(original_data)} 条数据，保留 {len(converted_data)} 条，跳过 {skipped_count} 条无效数据")
    print(f"结果已保存到 {output_file}")

def main():
    parser = argparse.ArgumentParser(description='JSON格式转换工具（自动过滤无效数据）')
    parser.add_argument('-i', '--input', required=True, help='输入JSON文件路径')
    parser.add_argument('-o', '--output', required=True, help='输出JSON文件路径')
    
    args = parser.parse_args()
    convert_json_format(args.input, args.output)

if __name__ == "__main__":
    main()