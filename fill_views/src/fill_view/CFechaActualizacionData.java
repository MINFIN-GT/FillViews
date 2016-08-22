package fill_view;

import java.sql.PreparedStatement;
import java.sql.Timestamp;

import org.joda.time.DateTime;
import utilities.CLogger;

public class CFechaActualizacionData {
	public static boolean UpdateLoadDate(String dashboard){
		DateTime date = new DateTime();
		boolean ret = false;
		try{
			if(CMemSQL.connect()){
				ret = true;
				CLogger.writeConsole("UdateLoadDate");											
				PreparedStatement pstm = CMemSQL.getConnection().prepareStatement("update update_log SET last_update = ? "
						+ " where dashboard_name = ? ");
				pstm.setTimestamp(1, new Timestamp(date.getMillis()));
				pstm.setString(2,dashboard);		
				ret = ret && pstm.executeUpdate()>0;
				pstm.close();				
			}			
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CFechaActualizacionData.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
