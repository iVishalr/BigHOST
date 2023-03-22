import os
import json
from typing import Dict

def preprocess_A1_output(teamId, assignmentId, output_path, key_path):
    return

def preprocess_A2_output(teamId, assignmentId, output_path, key_path, convergence_limit):
    if assignmentId == "A2T1":
        return

    if assignmentId == "A2T2":
        preprocessed_output = []
        output_file = open(output_path,"r")
        answer_file = open(key_path, "r")
        flag = True

        for output_line, answer_line in zip(output_file, answer_file):
            op_page, op_rank = output_line.strip().split(",")
            answer_page, answer_rank = answer_line.strip().split(",")

            if op_page != answer_page:
                flag = False
                break

            op_rank = float(op_rank)
            answer_rank = float(answer_rank)

            if abs(op_rank - answer_rank) <= convergence_limit:
                preprocessed_output.append((op_page, answer_rank))

        output_file.close()
        answer_file.close()

        if not flag:
            preprocessed_output = []
        else:
            for i in range(len(preprocessed_output)):
                page, rank = preprocessed_output[i]
                preprocessed_output[i] = f"{page},{rank:.2f}\n"

            output_file = open(output_path, "w")
            preprocessed_output = "".join(preprocessed_output)
            output_file.write(preprocessed_output)
            output_file.close()

def preprocess_A3_output(teamId, assignmentId, output_path, key_path):
    if not os.path.exists(output_path):
        return False

    with open(key_path) as f:
        answer_key = f.read()

    with open(output_path) as f:
        output = f.read()

    answer_key: Dict = json.loads(answer_key)

    try:
        output: Dict = json.loads(output)
    except:
        return False

    return answer_key == output