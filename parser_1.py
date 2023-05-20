import csv
import os

def parse_row(headers: list, row: list):
    obj = {}
    for i in range(min(len(headers), len(row))):
        obj[headers[i]] = row[i]
    return obj

def load_drug_info(data_folder: str):
    drug_map = {}
    with open(os.path.join(data_folder, '2-1. The general information of drugs associated with resistance.txt'), 'r', encoding='utf-8', errors='ignore') as f:
        data = csv.reader((line.replace('\0','') for line in f), delimiter='\t')
        headers = next(data)
        for row in data:
            obj = parse_row(headers, row)
            if obj.get('DrugBank_ID') != None and len(obj['DrugBank_ID']) > 2:
                drug_map[obj['Drug_ID']] = {"drug_id": "DRUGBANK:" + obj['DrugBank_ID'], "drug_name": obj['Drug_Name']}
    return drug_map

def load_disease_info(data_folder: str):
    disease_map = {}
    with open(os.path.join(data_folder, '3-1. The general information of disease related with resistance.txt'), 'r', encoding='utf-8', errors='ignore') as f:
        data = csv.reader((line.replace('\0','') for line in f), delimiter='\t')
        headers = next(data)
        for row in data:
            obj = parse_row(headers, row)
            if obj.get('Disease_ICD') != None and len(obj['Disease_ICD']) > 2:
                disease_map[obj['Disease_ID']] = {"disease_id": "ICD11:" + obj['Disease_ICD'].split(":")[-1].replace(' ', ''), "disease_name": obj['Disease_name']}
    return disease_map

def load_molecular_info(data_folder: str):
    molecular_map = {}
    with open(os.path.join(data_folder, '4-1. The general information of molecular associated with resistance.txt'), 'r', encoding='utf-8', errors='ignore') as f:
        data = csv.reader((line.replace('\0','') for line in f), delimiter='\t')
        headers = next(data)
        for row in data:
            obj = parse_row(headers, row)
            if obj.get('HGNC_ID') != None and len(obj['HGNC_ID']) > 2:
                 molecular_map[obj['Molecule_ID']] = {"molecule_id": obj.get("HGNC_ID"), "molecule_name": obj.get("Molecule_name"), "molecule_type": obj.get('Molecule_type'), "species": obj['Molecule_species']}
    return molecular_map

def transform_entry(parsed_row, drug_map, disease_map, molecular_map):
    if len(parsed_row.keys()) == 0:
        return None

    obj = {
        'subject': molecular_map.get(parsed_row.get('Molecule_ID')),
        'object': drug_map.get(parsed_row.get('Drug_ID')),
        'association': disease_map.get(parsed_row.get('Disease_ID'))
    }

    # HIV case
    if parsed_row.get('Disease_ID') == None:
        obj['association'] = {"disease_name": "HIV"}
    elif obj.get('Disease_ID') == None:
        obj['association'] = {}

    # adding more to assocation
    obj['association']['sensitivity'] = parsed_row['Drug_sensitivity']
    
    if obj['subject'] != None and obj['object'] != None and obj['association'] != None:
        return obj

def load_data(data_folder: str):
    drug_map = load_drug_info(data_folder)
    disease_map = load_disease_info(data_folder)
    molecular_map = load_molecular_info(data_folder)

    with open(os.path.join(data_folder, '1-1. The pair information of drug-disease (Besides HIV)-molecular based resistance.txt'), 'r', encoding='utf-8', errors='ignore') as f:
        data = csv.reader((line.replace('\0','') for line in f), delimiter='\t')
        headers = next(data)
        for row in data:
            obj = parse_row(headers, row)
            entry = transform_entry(obj, drug_map, disease_map, molecular_map)
            if entry != None:
                yield entry

    with open(os.path.join(data_folder, '1-11. The pair information of HIV-drug-molecular based resistance.txt'), 'r', encoding='utf-8', errors='ignore') as f:
        data = csv.reader((line.replace('\0','') for line in f), delimiter='\t')
        headers = next(data)
        for row in data:
            obj = parse_row(headers, row)
            entry = transform_entry(obj, drug_map, disease_map, molecular_map)
            if entry != None:
                yield entry
        
if __name__ == '__main__':
    count = 0
    for data in load_data('./'):
        print(data)
        count += 1
    print(count)