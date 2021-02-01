import urllib3
import json
import pandas as pd
from time import sleep
import os

code = ["morp", "wsd", "wsd_poly", "ner", "dparse", "srl"]
code_kor = ["형태소 분석(문어/구어)",
            "어휘의미 분석(동음이의어)(문어)",
            "어휘의미 분석(다의어)(문어)",
            "개체명 인식(문어/구어)",
            "의존 구문 분석(문어)",
            "의미역 인식(문어)"]

Error_codes = ["400", "413", "429", "404", "408", "500"]
Error_messages = ["필수 파라미터의 값이 없거나 유효하지 않습니다.",
                  "문장 또는 어휘의 크기가 너무 큽니다.",
                  "서비스를 짧은 시간내에 많이 요청하셨습니다.",
                  "등록되지 않은 서비스 입니다.",
                  "서버의 요청 대기가 시간을 초과했습니다.",
                  "서버에서 요청을 처리할 수 없습니다."]


# API KEY 불러오기
def api_key_set():
    api_keys = open("API_KEY_LIST.txt", 'r')
    global API_Key_list
    API_Key_list = []
    while True:
        line = api_keys.readline()
        if not line:
            break
        API_Key_list.append(line)
    api_keys.close()


# input_data 불러오기
def input_data():
    input_path = './Storage/input'
    input_files = os.listdir(input_path)
    print("Exobrain API를 사용하실 데이터셋을 선택해주세요. (숫자 입력)")
    for i in range(len(input_files)):
        print(i + 1, "번 : ", input_files[i])
    while True:
        input_check = int(input()) - 1
        if 0 <= input_check < len(input_files):
            break
    global file_name
    file_name = input_files[input_check]
    file_path = input_path + '/' + file_name
    global df
    print("파일을 읽는 중입니다...")
    df = pd.read_csv(file_path, error_bad_lines=False, header=None, names=["input_text"])
    print("파일 읽기를 완료하였습니다!!")


# step_size 설정하기
def set_step():
    print("작업량을 입력해주세요(default[enter 키] = 100)")
    global step_size
    step_size = 100
    while True:
        tmp = input()
        if tmp == "":
            break
        if tmp.isdecimal():
            step_size = int(tmp)
            break
        print("입력값에 오류가 있습니다. 다시 입력해주세요")


def set_code():
    global openApiURL
    openApiURL = "http://aiopen.etri.re.kr:8000/WiseNLU"
    print("분석할 작업을 선택해주세요 (숫자 입력, default[enter 키] = 형태소 분석)")
    for i in range(len(code_kor)):
        print(i + 1, "번 :", code_kor[i])

    code_check = 0
    while True:
        tmp = input()
        if tmp == "":
            break
        if tmp.isdecimal():
            code_check = int(tmp) - 1
            break
        print("입력값에 오류가 있습니다. 다시 입력해주세요")
    global analysisCode
    analysisCode = code[code_check]

    if code_check in [0, 3]:
        literary_style = ['문어', '구어']
        print("분석할 문장의 어체를 선택해주세요 (숫자 입력, default[enter 키] = 문어")
        print("1번. 문어     2번. 구어")
        style_check = 0
        while True:
            tmp = input()
            if tmp == "":
                break
            if tmp.isdecimal():
                if tmp == 2:
                    openApiURL += "_spoken"
                break
            print("입력값에 오류가 있습니다. 다시 입력해주세요")


# output_data 불러오기
def output_data():
    global save_path
    save_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/") + '/Storage/output' + '/' + analysisCode
    save_path = save_dir + '/' + file_name
    index = 0
    save_df = None
    if os.path.isfile(save_path):
        print("저장된 파일을 불러오는 중입니다..")
        save_df = pd.read_csv(save_path, error_bad_lines=False, header=None, names=["output_text"])
        index = len(save_df)
        print("파일 불러오기가 완료되었습니다!!")
    else:
        save_df = pd.DataFrame(columns=["output_text"])
        if not (os.path.isdir(save_dir)):
            os.makedirs(os.path.join(save_dir))
    return save_df, index


def printProgressBar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', printEnd="\r"):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
    if iteration == total:
        print()


def api_request(save_df, index):
    print("API request 중...")
    key_index = 0
    printProgressBar(0, step_size, prefix='Progress:', suffix='Complete', length=50)
    progress_check = 0
    for item in df["input_text"].iloc[index:step_size + index].iteritems():
        progress_check += 1
        printProgressBar(progress_check, step_size, prefix='Progress:', suffix='Complete', length=50)
        access_key = API_Key_list[key_index]
        text = item[1]
        # request를 보내기 위한 변수값 설정
        requestJson = {
            "access_key": access_key,
            "argument": {
                "text": text,
                "analysis_code": analysisCode
            }
        }
        # 서버로 request를 보내 response값을 받아온다.
        http = urllib3.PoolManager()
        response = http.request(
            "POST",
            openApiURL,
            headers={"Content-Type": "application/json; charset=UTF-8"},
            body=json.dumps(requestJson)
        )
        save_df.loc[index, 'output_text'] = str(response.data, "utf-8")
        index += 1
        # 에러 발생시 에러 문구 출력
        if str(response.status) in Error_codes:
            print("Error 발생 : " + Error_messages[Error_codes.index(str(response.status))])
            print("Error 발생 위치 : line" + str(index))
            print("Error 발생 문구 : " + df["input_text"].iloc[index])
            if key_index < len(API_Key_list):
                key_index += 1
        # 과도하게 빠른 request를 보내는걸 방지 하기 위한 delay 설정
        sleep(0.3)

    return save_df


def main():
    api_key_set()
    input_data()
    set_step()
    set_code()
    save_df, index = output_data()
    save_df = api_request(save_df, index)
    save_df.to_csv(save_path, index=False, header=False)
    print("분석된 데이터의 저장이 완료되었습니다.")


if __name__ == '__main__':
    main()
