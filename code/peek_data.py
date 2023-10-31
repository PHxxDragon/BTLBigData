import json 

DATA_FILE_PATH = "2023-10-30-15.json"
DATA_OUT = "output.txt"

def test_print_json(input_json) -> str:
    return json.dumps(input_json, indent=4)

def main() -> None:
    dict_type_jsonstr: dict[str, str] = dict()
    dict_type_action_jsonstr: dict[str, dict[str, str]] = dict()

    with open(DATA_FILE_PATH, 'r') as data_file:
        for line in data_file.readlines():
            input_json = json.loads(line)
            typ = input_json["type"]
            if (typ not in dict_type_jsonstr):        
                if ("action" not in input_json):
                    dict_type_jsonstr[typ] = f"{test_print_json(input_json)}\n"
                else:
                    if (typ not in dict_type_action_jsonstr):
                        dict_type_action_jsonstr[typ] = dict()

                    dict_action = dict_type_action_jsonstr[typ]
                    act = input_json["action"]
                    if (act not in dict_action):
                        dict_action[act] = f"{test_print_json(input_json)}\n"

    with open(DATA_OUT, 'w') as data_out:
        for _, jsonstr in dict_type_jsonstr.items():
            data_out.write(jsonstr)
        for _, dict_action_jsonstr in dict_type_action_jsonstr.items():
            for _, jsonstr in dict_action_jsonstr.items():
                data_out.write(jsonstr)
        data_out.flush()

if __name__ == "__main__":
    main()