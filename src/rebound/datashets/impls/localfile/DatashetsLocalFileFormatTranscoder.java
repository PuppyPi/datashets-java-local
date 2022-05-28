package rebound.datashets.impls.localfile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import rebound.datashets.api.model.DatashetsTable;
import rebound.exceptions.BinarySyntaxException;

public interface DatashetsLocalFileFormatTranscoder
{
	public DatashetsTable read(InputStream in) throws IOException, BinarySyntaxException;
	
	public void write(DatashetsTable data, OutputStream out) throws IOException;
}
