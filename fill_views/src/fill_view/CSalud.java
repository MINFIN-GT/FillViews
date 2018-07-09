package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;

public class CSalud {
	
	public static boolean updateHospitales(Connection conn){
		boolean ret = true;
		try{
			CLogger.writeConsole("CSalud (Actualizacion de vista de Hospitales):");
			CLogger.writeConsole("Elminiando la data actual de mv_hospitales");
			PreparedStatement pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_hospitales");
			pstm.executeUpdate();
			pstm.close();
			
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_hospitales " +
					"select t.ejercicio,h.unidad_ejecutora, h.nombre, r.rubro, r.nombre nombre_rubro, r.orden, r.nivel, d.nombre_departamento, d.codigo_departamento,   " + 
					"  sum(case when ep.mes = 1 then asignado end) asignado, sum(ano_actual) ejecucion, sum(case when ep.mes=12 then vigente end) vigente " + 
					"  from observatorio.hospitales_tercer_nivel h cross join observatorio.hospitales_rubro r left outer join " + 
					"  observatorio.hospitales_rubro_renglon rr on (r.rubro == rr.rubro) cross join dashboard.tiempo t left outer join " + 
					"  dashboard.mv_ejecucion_presupuestaria ep on ( " + 
					"    ep.ejercicio = t.ejercicio " + 
					"    and ep.unidad_ejecutora = h.unidad_ejecutora " + 
					"    and ep.entidad = 11130009 " + 
					"    and ep.renglon = rr.renglon  " + 
					"  ), sicoinprod.cg_departamentos d " + 
					"  where t.ejercicio between year(current_timestamp)-4 and year(current_timestamp) " + 
					"  and t.mes = 1 " + 
					"  and t.dia = 1 " + 
					"  and d.codigo_departamento = h.departamento " + 
					"  group by t.ejercicio,h.unidad_ejecutora, h.nombre, r.rubro, r.nombre, r.orden, r.nivel, d.nombre_departamento, d.codigo_departamento " + 
					"  order by t.ejercicio,codigo_departamento, unidad_ejecutora, nombre, orden, rubro, nombre_rubro, nivel");
			pstm.executeUpdate();
			pstm.close();
			
			if(!conn.isClosed()){
				pstm = conn.prepareStatement("select * " + 
						"from dashboard.mv_hospitales");
				
				ResultSet rs = pstm.executeQuery();
				boolean bconn = CMemSQL.connect();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CSalud (load mv_hospitales):");
					PreparedStatement pstm2;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_hospitales (ejercicio, unidad_ejecutora, nombre, rubro, nombre_rubro,"
							+ "orden, nivel, nombre_departamento, codigo_departamento, asignado, ejecucion, vigente) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?) ");
					while(rs.next()){
						if(first){
							pstm2 = CMemSQL.getConnection().prepareStatement("truncate table mv_hospitales");
							pstm2.executeUpdate();
							CLogger.writeConsole("Registros eliminados");
							pstm2.close();
							first=false;
						}
						
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("unidad_ejecutora"));
						pstm1.setString(3,rs.getString("nombre"));
						pstm1.setInt(4, rs.getInt("rubro"));
						pstm1.setString(5,rs.getString("nombre_rubro"));
						pstm1.setInt(6, rs.getInt("orden"));
						pstm1.setInt(7, rs.getInt("nivel"));
						pstm1.setString(8,rs.getString("nombre_departamento"));
						pstm1.setInt(9, rs.getInt("codigo_departamento"));
						pstm1.setDouble(10, rs.getDouble("asignado"));
						pstm1.setDouble(11, rs.getDouble("ejecucion"));
						pstm1.setDouble(12, rs.getDouble("vigente"));
						pstm1.addBatch();
						
						rows++;
						if((rows % 10000) == 0){
							pstm1.executeBatch();
							CLogger.writeConsole(String.join("Records escritos: ",String.valueOf(rows)));
						}
					}	
					pstm1.executeBatch();
					CLogger.writeConsole(String.join("Total de records escritos: ",String.valueOf(rows)));
					rs.close();
					pstm1.close();
					pstm.close();
				}
				
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CSalud.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	public static boolean updateCentros(Connection conn){
		boolean ret = true;
		try{
			CLogger.writeConsole("CSalud (Actualizacion de vista de Centros de Salud):");
			CLogger.writeConsole("Elminiando la data actual de mv_centros_salud");
			PreparedStatement pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_centros_salud");
			pstm.executeUpdate();
			pstm.close();
			
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_centros_salud " +
					"select t.ejercicio,m.codigo_municipio, m.nombre_municipio, d.nombre_departamento, d.codigo_departamento, r.rubro, r.nombre nombre_rubro, r.orden, r.nivel,  " + 
					"					 sum(case when epg.mes = 1 then asignado end) asignado, sum(ano_actual) ejecucion, sum(case when epg.mes = 12 then vigente end) vigente   " + 
					"					 from (select codigo_departamento, codigo_municipio, cast(concat(codigo_departamento,lpad(codigo_municipio,2,'0')) as int) geografico, " + 
					"              nombre_municipio  from sicoinprod.cg_municipios) m cross join observatorio.hospitales_rubro r left outer join   " + 
					"					 observatorio.hospitales_rubro_renglon rr on (r.rubro == rr.rubro) cross join dashboard.tiempo t left outer join   " + 
					"					 dashboard.mv_ejecucion_presupuestaria_geografico epg on (   " + 
					"					 epg.ejercicio = t.ejercicio   " + 
					"					 and epg.geografico = m.geografico   " + 
					"					 and epg.entidad = 11130009   " + 
					"					 and epg.programa in (13,16,17) " + 
					"					 and epg.renglon = rr.renglon    " +
					"					 and (epg.unidad_ejecutora not in (227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,250,252,253,254,255,259,260,261,262,263,264,285)) " +
					"					 and (epg.unidad_ejecutora not in (201,272, 273, 274, 275, 276, 277, 279, 280, 281)) " +
					"					 ), sicoinprod.cg_departamentos d  " + 
					"					 where t.ejercicio between year(current_timestamp)-4 and year(current_timestamp)   " + 
					"					 and t.mes = 1   " + 
					"					 and t.dia = 1   " + 
					"					 and m.codigo_departamento = d.codigo_departamento " + 
					"					 group by t.ejercicio,m.geografico, m.codigo_municipio, m.nombre_municipio, r.rubro, r.nombre, r.orden, r.nivel, d.nombre_departamento, d.codigo_departamento   " + 
					"					 order by t.ejercicio, codigo_municipio, codigo_departamento, orden, rubro, nombre_rubro, nivel");
			pstm.executeUpdate();
			pstm.close();
			
			if(!conn.isClosed()){
				pstm = conn.prepareStatement("select * " + 
						"from dashboard.mv_centros_salud");
				
				ResultSet rs = pstm.executeQuery();
				boolean bconn = CMemSQL.connect();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CSalud (load mv_centros_salud):");
					PreparedStatement pstm2;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_centros_salud (ejercicio, codigo_municipio, nombre_municipio, codigo_departamento, nombre_departamento,"
							+ "rubro, nombre_rubro, orden, nivel, asignado, ejecucion, vigente) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?) ");
					while(rs.next()){
						if(first){
							pstm2 = CMemSQL.getConnection().prepareStatement("truncate table mv_centros_salud");
							pstm2.executeUpdate();
							CLogger.writeConsole("Registros eliminados");
							pstm2.close();
							first=false;
						}
						
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("codigo_municipio"));
						pstm1.setString(3,rs.getString("nombre_municipio"));
						pstm1.setInt(4, rs.getInt("codigo_departamento"));
						pstm1.setString(5,rs.getString("nombre_departamento"));
						pstm1.setInt(6, rs.getInt("rubro"));
						pstm1.setString(7,rs.getString("nombre_rubro"));
						pstm1.setInt(8, rs.getInt("orden"));
						pstm1.setInt(9, rs.getInt("nivel"));
						pstm1.setDouble(10, rs.getDouble("asignado"));
						pstm1.setDouble(11, rs.getDouble("ejecucion"));
						pstm1.setDouble(12, rs.getDouble("vigente"));
						pstm1.addBatch();
						
						rows++;
						if((rows % 10000) == 0){
							pstm1.executeBatch();
							CLogger.writeConsole(String.join("Records escritos: ",String.valueOf(rows)));
						}
					}	
					pstm1.executeBatch();
					CLogger.writeConsole(String.join("Total de records escritos: ",String.valueOf(rows)));
					rs.close();
					pstm1.close();
					pstm.close();
				}
				
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 2: CSalud.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	public static boolean updatePuestos(Connection conn){
		boolean ret = true;
		try{
			CLogger.writeConsole("CSalud (Actualizacion de vista de Puestos de Salud):");
			CLogger.writeConsole("Elminiando la data actual de mv_puestos_salud");
			PreparedStatement pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_puestos_salud");
			pstm.executeUpdate();
			pstm.close();
			
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_puestos_salud " +
					"select epg.ejercicio,m.codigo_municipio, m.nombre_municipio, d.nombre_departamento, d.codigo_departamento, r.rubro, r.nombre nombre_rubro, r.orden, r.nivel,    " + 
					"    										 sum(case when epg.mes = 1 then asignado end) asignado, sum(ano_actual) ejecucion, sum(case when epg.mes = 12 then vigente end) vigente " + 
					"    from dashboard.mv_ejecucion_presupuestaria_geografico epg  " + 
					"    right outer join observatorio.hospitales_rubro_renglon rr on (rr.renglon = epg.renglon) " + 
					"    right outer join observatorio.hospitales_rubro r on (r.rubro = rr.rubro) " + 
					"    full outer join (select codigo_departamento, codigo_municipio, cast(concat(codigo_departamento,lpad(codigo_municipio,2,'0')) as int) geografico,   " + 
					"    					              nombre_municipio  from sicoinprod.cg_municipios) m on (epg.geografico=m.geografico)  " + 
					"    full outer join sicoinprod.cg_departamentos d on (m.codigo_departamento = d.codigo_departamento)          " + 
					"    where epg.entidad = 11130009 " + 
					"    and epg.programa in (12,14,15) " + 
					"    and (epg.unidad_ejecutora not in (227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,250,252,253,254,255,259,260,261,262,263,264,285)) " + 
					"    and (epg.unidad_ejecutora not in (201,272, 273, 274, 275, 276, 277, 279, 280, 281) or (epg.unidad_ejecutora=201 and epg.programa in (14,15) and epg.renglon=266)) " + 
					"    and epg.ejercicio between year(current_timestamp)-4 and year(current_timestamp) " + 
					"    group by epg.ejercicio,m.geografico, m.codigo_municipio, m.nombre_municipio, r.rubro, r.nombre, r.orden, r.nivel, d.nombre_departamento, d.codigo_departamento ");
			pstm.executeUpdate();
			pstm.close();
			
			if(!conn.isClosed()){
				pstm = conn.prepareStatement("select * " + 
						"from dashboard.mv_puestos_salud");
				
				ResultSet rs = pstm.executeQuery();
				boolean bconn = CMemSQL.connect();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CSalud (load mv_puestos_salud):");
					PreparedStatement pstm2;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_puestos_salud (ejercicio, codigo_municipio, nombre_municipio, codigo_departamento, nombre_departamento,"
							+ "rubro, nombre_rubro, orden, nivel, asignado, ejecucion, vigente) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?) ");
					while(rs.next()){
						if(first){
							pstm2 = CMemSQL.getConnection().prepareStatement("truncate table mv_puestos_salud");
							pstm2.executeUpdate();
							CLogger.writeConsole("Registros eliminados");
							pstm2.close();
							first=false;
						}
						
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("codigo_municipio"));
						pstm1.setString(3,rs.getString("nombre_municipio"));
						pstm1.setInt(4, rs.getInt("codigo_departamento"));
						pstm1.setString(5,rs.getString("nombre_departamento"));
						pstm1.setInt(6, rs.getInt("rubro"));
						pstm1.setString(7,rs.getString("nombre_rubro"));
						pstm1.setInt(8, rs.getInt("orden"));
						pstm1.setInt(9, rs.getInt("nivel"));
						pstm1.setDouble(10, rs.getDouble("asignado"));
						pstm1.setDouble(11, rs.getDouble("ejecucion"));
						pstm1.setDouble(12, rs.getDouble("vigente"));
						pstm1.addBatch();
						
						rows++;
						if((rows % 10000) == 0){
							pstm1.executeBatch();
							CLogger.writeConsole(String.join("Records escritos: ",String.valueOf(rows)));
						}
					}	
					pstm1.executeBatch();
					CLogger.writeConsole(String.join("Total de records escritos: ",String.valueOf(rows)));
					rs.close();
					pstm1.close();
					pstm.close();
				}
				
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 3: CSalud.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
