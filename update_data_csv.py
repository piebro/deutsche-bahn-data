import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path

def get_eva_to_name_dict():
    with Path("eva_name_list.txt").open("r") as f:
        eva_to_list = {line.split(",")[0]: line.split(",")[1] for line in f.read().split("\n")}
    return eva_to_list

def get_plan_xml_rows(xml_path, eva_to_name):
    eva = xml_path.name.split("_")[0]
    station = eva_to_name[eva]

    tree = ET.parse(xml_path)
    root = tree.getroot()
    rows = []
    for s in root.findall('s'):
        s_id = s.get('id')
        train_type = s.find('tl').get('c') if s.find('tl') is not None else None
        train_number = s.find('tl').get('n') if s.find('tl') is not None else None
        ar_train_line_number = s.find('ar').get('l') if s.find('ar') is not None else None
        dp_train_line_number = s.find('dp').get('l') if s.find('dp') is not None else None
        
        if train_type in ['IC', 'ICE', 'EC']:
            train_name = f"{train_type} {train_number}"
        else:
            if ar_train_line_number is not None:
                train_name = f"{train_type} {ar_train_line_number}"
            elif dp_train_line_number is not None:
                train_name = f"{train_type} {dp_train_line_number}"
            else:
                train_name = train_type
        
        ar_pp = s.find('ar').get('pp') if s.find('ar') is not None else None
        dp_pp = s.find('dp').get('pp') if s.find('dp') is not None else None
        planned_platform = ar_pp or dp_pp # `or` to select the first non-None value

        dp_ppth = s.find('dp').get('ppth') if s.find('dp') is not None else None # departure planed path
        if dp_ppth is None:
            destination_station = station
        else:
            destination_station = dp_ppth.split("|")[-1]
        
        s_id_split = s_id.split('-')

        rows.append({
            'id': s_id,
            'station': station,
            'train_name': train_name,
            'destination_station': destination_station,
            'train_number': int(train_number),
            'arrival_planned_time': s.find('ar').get('pt') if s.find('ar') is not None else None,
            'departure_planned_time': s.find('dp').get('pt') if s.find('dp') is not None else None,
            'planned_platform': planned_platform,
            'train_line_id': '-'.join(s_id_split[:-1]),
            'train_line_station_num': int(s_id_split[-1]),
            
            # 'arrival_planned_path': s.find('ar').get('ppth') if s.find('ar') is not None else None,
            # 'departure_planned_path': s.find('dp').get('ppth') if s.find('dp') is not None else None,

        })
    return rows

def get_plan_db():
    eva_to_name = get_eva_to_name_dict()
    rows = []
    for date_folder_path in Path("data").iterdir():
        for xml_path in sorted(date_folder_path.iterdir()):
            if "plan" in xml_path.name:
                rows.extend(get_plan_xml_rows(xml_path, eva_to_name))
    
    out_df = pd.DataFrame(rows)
    out_df['arrival_planned_time'] = pd.to_datetime(out_df['arrival_planned_time'], format='%y%m%d%H%M', errors='coerce')
    out_df['departure_planned_time'] = pd.to_datetime(out_df['departure_planned_time'], format='%y%m%d%H%M', errors='coerce')
    out_df = out_df.drop_duplicates()
    return out_df

def get_fchg_xml_rows(xml_path, id_to_data):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    for s in root.findall('s'):
        s_id = s.get('id')
        ar_ct = s.find('ar').get('ct') if s.find('ar') is not None else None    # arrival change 
        dp_ct = s.find('dp').get('ct') if s.find('dp') is not None else None    # departure change 
        ar_clt = s.find('ar').get('clt') if s.find('ar') is not None else None    # arrival cancellation time 
        dp_clt = s.find('dp').get('clt') if s.find('dp') is not None else None    # departure cancellation time 

        if ar_clt is None and dp_clt is None:
            stop_canceled = False
        else:
            stop_canceled = True
            ar_ct = None
            dp_ct = None
        
        # arrival or departure changed platform
        ar_cp = s.find('ar').get('cp') if s.find('ar') is not None else None
        dp_cp = s.find('dp').get('cp') if s.find('dp') is not None else None
        changed_platform = ar_cp or dp_cp
        
        if ar_ct is None and dp_ct is None and changed_platform is None and not stop_canceled:
            continue
        
        # overwrite older data with new data
        id_to_data[s_id] = {
            'id': s_id,
            'arrival_change_time': ar_ct,
            'departure_change_time': dp_ct,
            'stop_canceled': stop_canceled,
            'changed_platform': changed_platform,
        }

def get_fchg_db():
    id_to_data = {}
    for date_folder_path in Path("data").iterdir():
        for xml_path in sorted(date_folder_path.iterdir()): # get the oldest data first
            if "fchg" in xml_path.name:
                get_fchg_xml_rows(xml_path, id_to_data)
    
    out_df = pd.DataFrame(id_to_data.values())
    out_df['arrival_change_time'] = pd.to_datetime(out_df['arrival_change_time'], format='%y%m%d%H%M', errors='coerce')
    out_df['departure_change_time'] = pd.to_datetime(out_df['departure_change_time'], format='%y%m%d%H%M', errors='coerce')
    out_df = out_df.drop_duplicates()
    return out_df

def main():
    plan_df = get_plan_db()
    fchg_df = get_fchg_db()
    df = pd.merge(plan_df, fchg_df, on='id', how='left')

    df.loc[df["arrival_planned_time"] == df["arrival_change_time"], "arrival_change_time"] = None
    df.loc[df["departure_planned_time"] == df["departure_change_time"], "departure_change_time"] = None
    df.loc[df["stop_canceled"].isna(), "stop_canceled"] = False
    df = df.drop("id", axis=1)
    df.to_csv("data.csv", index=False)

if __name__ == "__main__":
    main()
