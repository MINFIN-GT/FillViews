package utilities;

import org.joda.time.DateTime;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class CDimensionTiempo {

		public static void createDimension(Connection conn, int init_year, int end_year){
			DateTime init = new DateTime(init_year, 1, 1, 0, 0); 
			DateTime end = new DateTime(end_year+1,1,1,0,0);
			try{
				if(!conn.isClosed()){
					PreparedStatement pstm_delete = conn.prepareStatement("DELETE FROM dashboard.tiempo WHERE ejercicio BETWEEN ? AND ?");
					pstm_delete.setInt(1, init_year);
					pstm_delete.setInt(2, end_year);
					pstm_delete.executeUpdate();
					long cont=0;
					PreparedStatement pstm = conn.prepareStatement("INSERT INTO dashboard.tiempo VALUES(?,?,?,?,?,?,?,?)");
					while(init.getMillis()<end.getMillis()){
						pstm.setInt(1, init.getDayOfMonth());
						pstm.setInt(2, init.getWeekOfWeekyear());
						pstm.setInt(3, init.getMonthOfYear());
						pstm.setInt(4, init.getMonthOfYear() < 4 ? 1 :( init.getMonthOfYear() < 7 ? 2  : (init.getMonthOfYear()<10 ? 3 : 4)));
						pstm.setInt(5, init.getMonthOfYear() < 5 ?  1 : (init.getMonthOfYear() < 9 ? 2 : 3));
						pstm.setInt(6, init.getYear());
						pstm.setLong(7, init.getMillis());
						pstm.setLong(8, init.plusHours(23).plusMinutes(59).plusSeconds(59).plusMillis(999).getMillis());
						init = init.plusDays(1);
						cont++;
						pstm.executeUpdate();
						if(cont%100==0)
							CLogger.writeConsole(cont+" Registros insertados");
					}
					pstm.close();
					CLogger.writeConsole("Totald e registros insertados "+cont);
				}
			}
			catch(Exception e){
				CLogger.writeFullConsole("Error 1: CDimensionTiempo.class", e);
			}
		}
}
