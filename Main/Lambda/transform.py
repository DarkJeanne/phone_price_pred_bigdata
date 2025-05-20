import pickle
import ast
import os
from xgboost import XGBRegressor

BRAND_TO_NUMERIC = {
    'Maxfone': 1, 'Infinix': 2, 'Freeyond': 3, 'XIAOMI': 4,
    'Tecno': 5, 'Oppo': 6, 'Nokia': 7, 'Samsung': 8,
    'Huawei': 9, 'Vivo': 10, 'Realme': 11, 'Sowhat': 12,
    'Apple': 13
}
NUMERIC_TO_BRAND = {v: k for k, v in BRAND_TO_NUMERIC.items()}
DEFAULT_BRAND_NUMERIC = 14
DEFAULT_BRAND_STRING = 'Unknown'

SIM_TYPE_TO_NUMERIC = {
    'Dual': 1, 'Single': 2
}
NUMERIC_TO_SIM_TYPE = {v: k for k, v in SIM_TYPE_TO_NUMERIC.items()}
DEFAULT_SIM_TYPE_NUMERIC = 3
DEFAULT_SIM_TYPE_STRING = 'Unknown'

def map_brand_to_numeric(brand):
    return BRAND_TO_NUMERIC.get(brand, DEFAULT_BRAND_NUMERIC)

def map_sim_type_to_numeric(sim_type):
    return SIM_TYPE_TO_NUMERIC.get(sim_type, DEFAULT_SIM_TYPE_NUMERIC)

def map_numeric_to_brand(number):
    return NUMERIC_TO_BRAND.get(number, DEFAULT_BRAND_STRING)

def map_numeric_to_sim_type(number):
    return NUMERIC_TO_SIM_TYPE.get(number, DEFAULT_SIM_TYPE_STRING)

def transformation(original_list):
    model = None
    possible_paths = [
        os.path.join(os.path.dirname(__file__), 'ML_operations', 'xgb_model.pkl'),
        os.path.join(os.path.dirname(__file__), 'ml_ops', 'xgb_model.pkl'),
        os.path.join(os.getcwd(), 'xgb_model.pkl'),
        os.path.join(os.getcwd(), 'ML_operations', 'xgb_model.pkl'),
        '/opt/app/ML_operations/xgb_model.pkl',
        '/opt/spark/apps/ml_ops/xgb_model.pkl',
        'xgb_model.pkl'
    ]
    
    for path in possible_paths:
        try:
            if os.path.exists(path):
                print(f"Found model at {path}")
                with open(path, 'rb') as f:
                    model = pickle.load(f)
                break
        except Exception as e:
            print(f"Error loading model from {path}: {e}")
    
    if model is None:
        print(f"Could not find model file in any of the expected locations: {possible_paths}")
        return []
        
    print(original_list)
    if isinstance(original_list, str):
        try:
            original_list = ast.literal_eval(original_list)
        except (ValueError, SyntaxError) as e:
            print(f"Error evaluating string to list: {e}. Returning empty list.")
            return []
    
    if not isinstance(original_list, list) or len(original_list) < 9:
        print(f"Invalid input format or insufficient data: {original_list}. Returning empty list.")
        return []

    brand_value = original_list[1] if len(original_list) > 1 else None
    screen_size_value = original_list[3] if len(original_list) > 3 else '0'
    ram_value = original_list[4] if len(original_list) > 4 else '0'
    storage_value = original_list[5] if len(original_list) > 5 else '0'
    sim_type_value = original_list[7] if len(original_list) > 7 else None
    battery_value = original_list[8] if len(original_list) > 8 else '0'

    try:
        new_list = [
            map_brand_to_numeric(brand_value),
            float(screen_size_value),
            float(ram_value),
            float(storage_value),
            map_sim_type_to_numeric(sim_type_value),
            float(battery_value)
        ]
    except ValueError as e:
        print(f"Error converting to float: {e}. Input: {original_list}. Returning empty list.")
        return []

    print(new_list)

    price = model.predict([new_list])

    new_list[0] = map_numeric_to_brand(new_list[0])
    new_list[4] = map_numeric_to_sim_type(new_list[4])

    print(new_list)

    new_list.append(float(price[0]))

    return new_list


