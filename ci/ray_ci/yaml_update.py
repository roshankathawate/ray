import argparse
import yaml
import json
import ast


def update_yaml(yaml_file: str, updates: dict):

    with open(yaml_file, "r") as file:
        data = yaml.safe_load(file)

    for key, value in updates.items():

        if "available_node_types.ray.head.default" in key:
            keys = key.split(".")
            keys[1] = ".".join([keys[1], keys[2], keys[3]])
            keys.pop(2)
            keys.pop(2)
        else:
            keys = key.split(".")

        current_data = data
        for k in keys[:-1]:
            current_data = current_data.setdefault(k, {})

        if "[" in value:
            value = ast.literal_eval(value)
            current_data[keys[-1]] = value
        elif "{" in value:
            current_data[keys[-1]] = json.loads(value)
        elif "false" in value.lower() or "true" in value.lower():
            boolean_value = value.lower() == "true"
            current_data[keys[-1]] = boolean_value
        else:
            current_data[keys[-1]] = value

    with open(yaml_file, "w") as file:
        yaml.dump(data, file, default_flow_style=False)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Update YAML file")
    parser.add_argument("file", help="Path to the YAML file")
    parser.add_argument(
        "updates",
        nargs="+",
        help="Key-value pairs to update (key1=value1 key2=value2 ...)",
    )

    args = parser.parse_args()

    updates_dict = dict(update.split("=", 1) for update in args.updates)

    update_yaml(args.file, updates_dict)
