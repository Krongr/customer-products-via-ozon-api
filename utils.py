from datetime import datetime


def write_event_log(event, function_name:str, additional_info:str=None):
    _date = datetime.now().date()
    _time = datetime.now().strftime("%H:%M:%S")
    if additional_info:
        with open(f'_logs/{_date}.txt', 'a', encoding='utf-8') as file:
            file.write(f'--{_time} | {function_name}\n{event}\n'
                       f'{additional_info}\n')
    else:
        with open(f'_logs/{_date}.txt', 'a', encoding='utf-8') as file:
            file.write(f'--{_time} | {function_name}\n{event}\n')
