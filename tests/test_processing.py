import io
import zipfile

from app.sheets_service import process_zip, COLUMNS


def test_process_zip_filters_and_headers():
    csv_content = """Receiver type,Current Station,TO Number,SPX Tracking Number,Receiver Name,TO Order Quantity,Operator,Create Time,Complete Time,Remark,Receive Status,Staging Area ID\n"
    csv_content += "Station,SOC 5,TO1,TRK1,Recv1,1,Op,2024-01-01,2024-01-02,Ok,Done,STA\n"
    csv_content += "Station,SOC 1,TO2,TRK2,Recv2,2,Op2,2024-01-03,2024-01-04,Ok,Done,STB\n"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("test.csv", csv_content)

    rows = process_zip(buf.getvalue())
    assert rows[0] == COLUMNS
    assert len(rows) == 2
    assert rows[1][0] == "TO1"
