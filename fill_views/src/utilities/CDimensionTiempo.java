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
					String query="INSERT INTO dashboard.tiempo VALUES ";
					String values="";
					PreparedStatement pstm = conn.prepareStatement("");
					while(init.getMillis()<end.getMillis()){
						values = values + String.join("",",("+init.getDayOfMonth()+",",
								init.getWeekOfWeekyear()+",",
								init.getMonthOfYear()+",", init.getMonthOfYear() < 4 ? "1" :( init.getMonthOfYear() < 7 ? "2"  : (init.getMonthOfYear()<10 ? "3" : "4")),",",
								init.getMonthOfYear() < 5 ?  "1" : (init.getMonthOfYear() < 9 ? "2" : "3"),",",
								init.getYear()+",",
								init.getMillis()+",", 
								init.plusHours(23).plusMinutes(59).plusSeconds(59).plusMillis(999).getMillis()+")");
						init = init.plusDays(1);
						cont++;
						if(cont%100==0) {
							pstm.executeUpdate(query + values.substring(1));
							values="";
							CLogger.writeConsole(cont+" Registros insertados");
						}
					}
					if(values.length()>0)
						pstm.executeUpdate(query + values.substring(1));
					pstm.close();
					CLogger.writeConsole("Totald e registros insertados "+cont);
				}
			}
			catch(Exception e){
				CLogger.writeFullConsole("Error 1: CDimensionTiempo.class", e);
			}
		}
}
