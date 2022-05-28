package rebound.datashets.impls.localfile;

import static java.util.Objects.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import rebound.datashets.api.model.DatashetsTable;
import rebound.datashets.api.operation.DatashetsConnection;
import rebound.datashets.api.operation.DatashetsOperation;
import rebound.datashets.api.operation.DatashetsOperation.DatashetsOperationWithDataTimestamp;
import rebound.datashets.api.operation.DatashetsStructureException;
import rebound.file.FSUtilities;

public class DatashetsConnectionForLocalFile
implements DatashetsConnection
{
	protected final @Nonnull File file;
	protected final @Nonnull DatashetsLocalFileFormatTranscoder fileFormat;
	
	public DatashetsConnectionForLocalFile(@Nonnull File file, @Nonnull DatashetsLocalFileFormatTranscoder fileFormat)
	{
		this.file = requireNonNull(file);
		this.fileFormat = requireNonNull(fileFormat);
	}
	
	public File getFile()
	{
		return file;
	}
	
	public DatashetsLocalFileFormatTranscoder getFileFormat()
	{
		return fileFormat;
	}
	
	
	
	@Override
	public Date getCurrentLastModifiedTimestamp() throws IOException
	{
		return file.isFile() ? new Date(file.lastModified()) : null;
	}
	
	
	
	@Override
	public void perform(boolean performMaintenance, DatashetsOperation operation) throws DatashetsStructureException, IOException
	{
		//todolp actually use maxRowsToRead in this ^^'
		
		@Nullable DatashetsTable input;
		@Nullable Date lastModifiedTimeOfOriginalData;
		
		if (file.isFile())
		{
			lastModifiedTimeOfOriginalData = new Date(file.lastModified());
			
			try (InputStream in = new FileInputStream(file))
			{
				input = fileFormat.read(in);
			}
		}
		else
		{
			input = null;
			lastModifiedTimeOfOriginalData = null;
		}
		
		
		
		@Nullable DatashetsTable output = operation instanceof DatashetsOperationWithDataTimestamp ? ((DatashetsOperationWithDataTimestamp)operation).performInMemory(input, lastModifiedTimeOfOriginalData) : operation.performInMemory(input);
		
		
		
		if (output != null)
		{
			FSUtilities.performSafeFileSystemWriteTwoStageAndCopy(file, out ->
			{
				fileFormat.write(output, out);
			});
		}
	}
}
