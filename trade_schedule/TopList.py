
from config.tushare_utils import pro
from value.value import today_, today
from utils import stockAnalysis
# 示例2：合并模式（自定义文件名）
import os

# 获取当前文件的绝对路径，并拼接目标目录
current_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(current_dir, "top_list_files")

# 确保目录存在
os.makedirs(output_dir, exist_ok=True)
def export_top_list(start_date, end_date, output_dir='top_list_files',
                    overwrite=False, single_file=False, combined_filename=None):
    """
    导出指定日期区间内的龙虎榜数据到文本文件

    参数：
    start_date : str
        开始日期 (格式：YYYYMMDD)
    end_date : str
        结束日期 (格式：YYYYMMDD)
    output_dir : str, 可选
        输出目录 (默认: 'top_list_files')
    overwrite : bool, 可选
        是否覆盖已存在的文件 (默认: False)
    single_file : bool, 可选
        是否合并到单个文件 (默认: False)
    combined_filename : str, 可选
        合并文件名 (默认: combined_起始日期_结束日期.txt)
    """
    try:
        # 获取交易日历
        cal = pro.trade_cal(start_date=start_date, end_date=end_date)
        trade_dates = cal[cal['is_open'] == 1]['cal_date'].tolist()

        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)

        if single_file:
            # 确定合并文件名
            if combined_filename is None:
                combined_filename = f"combined_{start_date}_{end_date}.txt"
            combined_path = os.path.join(output_dir, combined_filename)

            # 检查文件存在性
            if not overwrite and os.path.exists(combined_path):
                print(f"合并文件 {combined_filename} 已存在，跳过...")
                return

            all_codes = set()  # 初始化为集合
            for trade_date in trade_dates:
                try:
                    df = pro.top_list(trade_date=trade_date)
                    if not df.empty:
                        # 过滤条件保持不变
                        df_filtered = df[
                            df['ts_code'].astype(str).str.startswith(('60', '00')) &
                            ~df['name'].str.contains('ST', case=False, na=False)
                            ]
                        codes = df_filtered['ts_code'].astype(str).tolist()
                        all_codes.update(codes)  # 改用集合的update方法
                        print(f"{trade_date} 处理完成，找到 {len(codes)} 条记录")
                    else:
                        print(f"{trade_date} 无数据")
                except Exception as e:
                    print(f'处理 {trade_date} 时发生错误: {str(e)}')

            # 写入合并文件
            with open(combined_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(all_codes))
            print(f"合并文件已保存到 {combined_path}，总记录数：{len(all_codes)}")

        else:
            # 处理每日独立文件
            for trade_date in trade_dates:
                file_path = os.path.join(output_dir, f'{trade_date}.txt')

                if not overwrite and os.path.exists(file_path):
                    print(f'{trade_date}.txt 已存在，跳过...')
                    continue

                try:
                    df = pro.top_list(trade_date=trade_date)
                    if not df.empty:
                        # 过滤条件
                        df_filtered = df[
                            df['ts_code'].astype(str).str.startswith(('60', '00')) &
                            ~df['name'].str.contains('ST', case=False, na=False)
                            ]
                        # 写入文件
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write('\n'.join(df_filtered['ts_code'].astype(str)))
                        print(f'{trade_date}.txt 导出完成，找到 {len(df_filtered)} 条记录')
                    else:
                        # 创建空文件保持一致性
                        open(file_path, 'w').close()
                        print(f'{trade_date}.txt 导出完成，无数据')

                except Exception as e:
                    print(f'处理 {trade_date} 时发生错误: {str(e)}')

    except Exception as e:
        print(f'初始化时发生错误: {str(e)}')


# 调用函数
export_top_list(
    stockAnalysis.get_date_by_step(today_,-20).replace('-',''),
    today,
    single_file=True,
    output_dir=output_dir,  # 使用动态生成的路径
    combined_filename="all_stocks.txt",
    overwrite=True
)

# 示例3：独立文件模式
# export_top_list('20250403', '20250403', overwrite=True)