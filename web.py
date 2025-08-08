from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory
import os
import pandas as pd
from werkzeug.utils import secure_filename

web = Flask(__name__)
web.secret_key = 'supersecretkey'

# Define folders
web.config['BOM_FOLDER'] = os.path.join('uploads', 'bom')
web.config['SPEC_FOLDER'] = os.path.join('uploads', 'spec_df')
web.config['MASTER_FOLDER'] = os.path.join('uploads', 'master')
web.config['SALES_FOLDER'] = os.path.join('uploads', 'sales')
ALLOWED_EXTENSIONS = {'xlsx'}

# Create folders
for folder in [web.config['BOM_FOLDER'], web.config['SPEC_FOLDER'], web.config['MASTER_FOLDER'], web.config['SALES_FOLDER']]:
    os.makedirs(folder, exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# ✅ Fixed BOM reader
def read_bom_file(path):

    bom_df = pd.read_excel(path, sheet_name="BOM")

    bom_df['Consumption per Consumer IT'] = bom_df['Consumption per case'] / bom_df['Number per case']
    return bom_df  # << MUST return a DataFrame

def read_material_spec(path):

    spec_df = pd.read_excel(path, sheet_name="Material Spec Sheet")

    # master_df = pd.read_excel(path, sheet_name="Master Sheet")
    master_df = pd.DataFrame({
        'Cleaned Material Environment': [  # ✅ This is correct
            'HDPE- High-Density Polyethylene',
            'NA',
            'PP- Polypropylene',
            'PET-Polyethylene Terephthalate',
            'AL- Aluminium',
            'LDPE- Low-Density Polyethylene'
        ],
        'Classification': [
            'Plastic',
            'NA',
            'Plastic',
            'Plastic',
            'Metal',
            'Plastic'
        ]
    })



    spec_df['MOC'] = spec_df['MOC'].fillna('NA')
    spec_df['Cleaned Material Type'] = spec_df['Cleaned Material Type'].fillna('NA')
    spec_df['Weight in Gram'] = spec_df['Weight in Gram'].fillna(0)

    spec_df = pd.merge(spec_df, master_df[['Cleaned Material Environment', 'Classification']],
                   left_on='Cleaned Material Type', right_on='Cleaned Material Environment', how='left')

    spec_df = spec_df.rename(columns={'Classification': 'Material Class'})
    spec_df['Material Class'] = spec_df['Material Class'].fillna('NA')

    pivot_df = spec_df.groupby(['pm id', 'Material Class', 'Rigid / Flexible'])['MOC %'].sum().reset_index()
    pivot_df = pivot_df.pivot(index=['pm id', 'Rigid / Flexible'], columns='Material Class', values='MOC %').fillna(0).round().reset_index()

    columns_to_sum = ['Metal', 'NA', 'Plastic']
    pivot_df['Total'] = pivot_df[columns_to_sum].sum(axis=1, numeric_only=True)
    pivot_df['Considered Under EPR?'] = pivot_df['Plastic'].apply(lambda x: 'Yes' if x > 0 else 'No')

    def classify_category(row):
        plastic = row.get('Plastic', None)
        rigid_flexible = str(row.get('Rigid / Flexible', '')).strip().lower()
        
        try:
            plastic = pd.to_numeric(plastic, errors='coerce')
            
            if (rigid_flexible in ['flexible', 'rigid']) and plastic == 0:
                return 'NA'
            elif rigid_flexible == 'rigid' and plastic == 100:
                return 'Cat I'
            elif rigid_flexible == 'flexible' and plastic == 100:
                return 'Cat II'
            elif rigid_flexible == 'flexible' and plastic < 100:
                return 'Cat III'
            else:
                return 'Unclassified'
        except Exception as e:
            return f'Error: {e}'

    pivot_df['EPR Categorisation'] = pivot_df.apply(classify_category, axis=1)
    spec_df = pd.merge(spec_df, pivot_df[['pm id', 'EPR Categorisation']], on='pm id', how='left')
    spec_df = spec_df.rename(columns={'EPR Categorisation': 'Category'})

    # Conversion table
    Conver = pd.DataFrame({
        'Container Capacity UOM': ['LT', 'KG', 'G', 'MG'],
        'Needs Conversion': ['No', 'No', 'Yes', 'Yes'],
        'Multiplying Factor': [1, 1, 0.001, 0.000001]
    })
    

    spec_df = pd.merge(spec_df, Conver, on='Container Capacity UOM', how='left')
    spec_df['Container Capacity in KG / LT'] = spec_df['Container Capacity in case of Rigids'] * spec_df['Multiplying Factor']

    def container_capacity(row):
        if row['Rigid / Flexible'].strip().lower() == 'flexible':
            return 'NA'
        size = row['Container Capacity in KG / LT']
        if size < 0.9:
            return 'containers < 0.9L'
        elif 0.9 <= size < 4.9:
            return 'containers > 0.9L and < 4.9L'
        elif size >= 4.9:
            return 'containers > 4.9L'
        else:
            return None

    spec_df['Container Capacity'] = spec_df.apply(container_capacity, axis=1)
    return spec_df

# def read_master_file(path):
#     return pd.read_excel(path, sheet_name="Master Sheet")

def process_sales_file(path):
    global bom_path
    global spec_path

    sales_df = pd.read_excel(path, sheet_name="Sales Data")
    bom_df = pd.read_excel(bom_path)
    spec_df = pd.read_excel(spec_path)
    # master_df = read_master_file(path)
    master_df = pd.DataFrame({
        'Cleaned Material Environment': [
            'HDPE- High-Density Polyethylene',
            'NA',
            'PP- Polypropylene',
            'PET-Polyethylene Terephthalate',
            'AL- Aluminium',
            'LDPE- Low-Density Polyethylene'
        ],
        'Classification': [
            'Plastic',
            'NA',
            'Plastic',
            'Plastic',
            'Metal',
            'Plastic'
        ]
    })

    sales_df['Quarter'] = sales_df['Period'].str.split(" ").str[0]
    sales_df['FY Year'] = sales_df['Period'].str.split(" ").str[1]
    sales_df.drop("Period", axis=1, inplace=True)
    sales_df['Month'] = sales_df['yyyymm'].astype(str).str[-2:]
    sales_df['Year'] = sales_df['yyyymm'].astype(str).str[:4]
    sales_df['Month'] = sales_df['Month'].replace({
        '01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun',
        '07': 'Jul', '08': 'Aug', '09': 'Sept', '10': 'Oct', '11': 'Nov', '12': 'Dec'
    })

    # sales_bom = pd.merge(sales_df, bom_df, left_on='INV_MATERIALCODE', right_on='SKU ID', how='left')

    # sales_bom_mat = pd.merge(sales_bom, spec_df, on='pm id', how='left')

    Sales_bom = pd.merge(sales_df,bom_df[['SKU ID', 'SKU Name', 'Family Code', 'Family Description', 'pm id',
       'MaterialDescription', 'Number per case', 'Consumption per case',
       'Consumption per Consumer IT']],left_on='INV_MATERIALCODE', right_on='SKU ID', how='left')

    Sales_bom_mat = pd.merge(
        Sales_bom,
        spec_df[
            ['pm id', 'Mother Code', 'Weight in Gram', 'MOC',
        'Cleaned Material Type', 'MOC %', 'Rigid / Flexible','Material Class', 'Category','Container Capacity']
        ],
        left_on='pm id',
        right_on='pm id',
        how='left'
)

    Sales_bom_mat['Footprint'] = (Sales_bom_mat['Consumption per Consumer IT'] * Sales_bom_mat['SalesQty'] * Sales_bom_mat['Weight in Gram'])  /1000000


    total_footprint = Sales_bom_mat['Footprint'].sum()
    total_row = pd.DataFrame([['' for _ in range(len(Sales_bom_mat.columns))]], columns=Sales_bom_mat.columns)
    total_row.at[0, 'SKU Name'] = 'Total'
    total_row.at[0, 'Footprint'] = float(total_footprint.round(3))

    Sales_bom_mat = pd.concat([Sales_bom_mat, total_row], ignore_index=True)
    
    output_file_with_data = 'Sales_bom_mat.xlsx'
    Sales_bom_mat.to_excel(output_file_with_data, index=False)

    return Sales_bom_mat

# Routes
@web.route('/')
def upload_form():
    return render_template('upload.html')

@web.route('/upload_base_bom', methods=['POST'])
def upload_base_bom():
    global bom_path

    file = request.files.get('bom_file')
    if not file or not allowed_file(file.filename):
        flash('Invalid BOM file', 'error')
        return redirect(url_for('upload_form'))
    try:
        df = read_bom_file(file)
        filename = 'bom_' + secure_filename(file.filename)
        bom_path = os.path.join(web.config['BOM_FOLDER'], filename)
        df.to_excel(bom_path, index=False)
        preview = df.to_html(classes='display', index=False, table_id='bom_table')
        flash('BOM sheet uploaded successfully', 'success')
        return render_template('upload.html', bom_preview=preview)
    except Exception as e:
        flash(f'Error processing BOM: {e}', 'error')
        return redirect(url_for('upload_form'))

@web.route('/upload_spec_file', methods=['POST'])
def upload_spec_file():
    global spec_path

    file = request.files.get('spec_file')
    if not file or not allowed_file(file.filename):
        flash('Invalid Material Spec file', 'error')
        return redirect(url_for('upload_form'))
    try:
        df = read_material_spec(file)
        filename = 'spec_' + secure_filename(file.filename)
        spec_path = os.path.join(web.config['SPEC_FOLDER'], filename)
        df.to_excel(spec_path, index=False)
        preview = df.to_html(classes='display', index=False, table_id='spec_table')
        flash('Material Spec sheet uploaded successfully', 'success')
        return render_template('upload.html', spec_file_preview=preview)
    except Exception as e:
        flash(f'Error processing Material Spec Sheet: {e}', 'error')
        return redirect(url_for('upload_form'))

# @web.route('/upload_master_file', methods=['POST'])
# def upload_master_file():
#     file = request.files.get('master_file')
#     if not file or not allowed_file(file.filename):
#         flash('Invalid Master file', 'error')
#         return redirect(url_for('upload_form'))
#     try:
#         df = read_master_file(file)
#         filename = 'master_' + secure_filename(file.filename)
#         path = os.path.join(web.config['MASTER_FOLDER'], filename)
#         df.to_excel(path, index=False)
#         preview = df.to_html(classes='display', index=False, table_id='master_table')
#         flash('Master sheet uploaded successfully', 'success')
#         return render_template('upload.html', master_file_preview=preview)
#     except Exception as e:
#         flash(f'Error processing Master Sheet: {e}', 'error')
#         return redirect(url_for('upload_form'))

@web.route('/upload_sales_file', methods=['POST'])
def upload_sales_file():

    file = request.files.get('sales_file')
    if not file or not allowed_file(file.filename):
        flash('Invalid Sales file', 'error')
        return redirect(url_for('upload_form'))
    try:
        filename = 'sales_' + secure_filename(file.filename)
        path = os.path.join(web.config['SALES_FOLDER'], filename)
        file.save(path)
        df = process_sales_file(path)
        preview = df.to_html(classes='display', index=False, table_id='sales_table')
        flash('Sales sheet processed successfully', 'success')
        return render_template('upload.html', sales_file_preview=preview)
    except Exception as e:
        flash(f'Error processing Sales Sheet: {e}', 'error')
        return redirect(url_for('upload_form'))

@web.route('/download_sales_bom_mat', methods=['GET'])
def download_sales_bom_mat():
    download_folder = os.path.join(os.path.expanduser("~"), 'Downloads', 'Footprint')
    os.makedirs(download_folder, exist_ok=True)

    sales_folder = web.config['SALES_FOLDER']
    sales_files = [os.path.join(sales_folder, f) for f in os.listdir(sales_folder) if f.endswith('.xlsx')]
    if not sales_files:
        flash('No sales file found for processing.', 'error')
        return redirect(url_for('upload_form'))

    latest_sales_file = max(sales_files, key=os.path.getctime)
    df = process_sales_file(latest_sales_file)
    output_file_with_data = os.path.join(download_folder, 'Sales_bom_mat.xlsx')
    df.to_excel(output_file_with_data, index=False)

    return send_from_directory(download_folder, 'Sales_bom_mat.xlsx', as_attachment=True)

if __name__ == '__main__':
    web.run(debug=True, host='0.0.0.0', port=5007)
