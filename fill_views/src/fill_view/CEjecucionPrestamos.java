package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


import utilities.CLogger;

public class CEjecucionPrestamos {
	public static boolean loadEjecucionFinanciera(){
		CLogger.writeConsole("CEjecucionPrestamos (Ejecucion financiera)");
		String query = "select   " + 
				"	prestamo.ejercicio,  " + 
				"	prestamo.fuente, f.nombre fuente_nombre, " + 
				"	prestamo.organismo,  o.nombre organismo_nombre, " + 
				"	prestamo.correlativo, prestamo.prestamo_nombre, prestamo.prestamo_sigla, " + 
				"	p.entidad,  e.NOMBRE entidad_nombre, " + 
				"	p.unidad_ejecutora, ue.nombre unidad_ejecutora_nombre, " + 
				"	p.programa, prog.NOM_ESTRUCTURA programa_nombre, " + 
				"	p.subprograma, subp.NOM_ESTRUCTURA subprograma_nombre, " + 
				"	p.proyecto, proy.NOM_ESTRUCTURA proyecto_nombre, " + 
				"	p.obra, " + 
				"	p.actividad, act.NOM_ESTRUCTURA actividad_obra_nombre, " + 
				"	p.renglon, r.nombre renglon_nombre, p.geografico, g.nombre geografico_nombre " + 
				", sum(p.asignado)  asignado, " + 
				"(sum (p.asignado)+ sum (p.adicion)+ sum (p.disminucion)+ sum (p.traspaso_p)+ sum (p.traspaso_n)+ sum (p.transferencia_p)+ sum (p.transferencia_n))  vigente " + 
				" ,sum (gasto.monto_renglon)   ejecutado " + 
				"from ( " + 
				"	select fe.ejercicio, fe.fuente, fe.organismo, fe.correlativo, fe.nombre prestamo_nombre, fe.sigla prestamo_sigla " + 
				"	from dashboard.prestamo p, sicoinprod.cg_fuentes_especificas fe " + 
				"	where  p.fuente = fe.fuente  " + 
				"	and p.organismo = fe.organismo " + 
				"	and p.correlativo = fe.correlativo " + 
				"	and fe.ejercicio > year(p.fecha_inicio_ejecucion) -1 " + 
				"	and fe.ejercicio < year(p.fecha_fin_ejecucion) + 1 " + 
				") prestamo LEFT JOIN  sicoinprod.eg_f6_partidas p on ( " + 
				"	p.ejercicio = prestamo.ejercicio " + 
				"	and p.fuente = prestamo.fuente " + 
				"	and p.organismo = prestamo.organismo " + 
				"	and p.correlativo = prestamo.correlativo " + 
				")  " + 
				"  LEFT JOIN (SELECT DISTINCT p3.entidad,p3.ejercicio FROM sicoinprod.eg_f6_partidas p3 WHERE p3.unidad_ejecutora > 0) p2 " + 
				"   ON (p2.entidad = p.entidad and p2.ejercicio=prestamo.ejercicio " + 
				")  " + 
				"left join ( " + 
				"		select d.ejercicio, d.entidad, d.unidad_ejecutora, d.programa, d.subprograma, d.proyecto, d.obra, d.actividad, d.renglon, d.monto_renglon, d.fuente, d.organismo, d.correlativo, d.geografico " + 
				"		from sicoinprod.eg_gastos_hoja h, sicoinprod.eg_gastos_detalle d    " + 
				"		where h.ejercicio=d.ejercicio and h.entidad=d.entidad and h.unidad_ejecutora=d.unidad_ejecutora and h.no_cur=d.no_cur   " + 
				"		and h.estado='APROBADO' and h.clase_registro in ('DEV','CYD','REG','RDP')  " + 
				"		) gasto   " + 
				"		on (p.entidad=gasto.entidad and p.unidad_ejecutora=gasto.unidad_ejecutora   " + 
				"		and p.programa=gasto.programa and p.subprograma=gasto.subprograma and p.proyecto=gasto.proyecto and p.obra=gasto.obra  " + 
				"		and p.actividad=gasto.actividad and p.renglon=gasto.renglon and p.fuente = gasto.fuente and p.organismo=gasto.organismo and p.correlativo=gasto.correlativo " + 
				"		and gasto.ejercicio=prestamo.ejercicio " + 
				")  " + 
				",sicoinprod.cg_fuentes f, sicoinprod.cg_organismos o, sicoinprod.cg_entidades e,  " + 
				"sicoinprod.cg_entidades ue, sicoinprod.cp_estructuras prog, sicoinprod.cp_estructuras subp, sicoinprod.cp_estructuras proy,  " + 
				"sicoinprod.cp_estructuras act, sicoinprod.cp_objetos_gasto r, sicoinprod.cg_geograficos g " + 
				"where (p2.entidad IS NULL OR p.unidad_ejecutora > 0) " + 
				"	and f.ejercicio = p.ejercicio and f.fuente = p.fuente	 " + 
				"	and o.ejercicio = p.ejercicio and o.organismo = p.organismo " + 
				"	and e.ejercicio = p.ejercicio and e.entidad = p.entidad AND e.unidad_ejecutora = 0 " + 
				"    AND ue.ejercicio = p.ejercicio AND ue.entidad = p.entidad AND ue.unidad_ejecutora = p.unidad_ejecutora " + 
				"    AND prog.ejercicio = p.ejercicio AND prog.entidad = p.entidad AND prog.unidad_ejecutora = p.unidad_ejecutora AND prog.programa = p.programa AND prog.nivel_estructura = 2 " + 
				"    AND subp.ejercicio = p.ejercicio AND subp.entidad = p.entidad AND subp.unidad_ejecutora = p.unidad_ejecutora AND subp.programa = p.programa AND subp.subprograma = p.subprograma AND subp.nivel_estructura = 3 " + 
				"    AND proy.ejercicio = p.ejercicio AND proy.entidad = p.entidad AND proy.unidad_ejecutora = p.unidad_ejecutora AND proy.programa = p.programa AND proy.subprograma = p.subprograma AND proy.proyecto = p.proyecto AND proy.nivel_estructura = 4 " + 
				"    AND act.ejercicio = p.ejercicio AND act.entidad = p.entidad AND act.unidad_ejecutora = p.unidad_ejecutora AND act.programa = p.programa AND act.subprograma = p.subprograma AND act.proyecto = p.proyecto AND act.obra = p.obra AND act.actividad = p.actividad AND act.nivel_estructura = 5 " + 
				"    AND r.ejercicio = p.ejercicio AND r.renglon = p.renglon " + 
				" 	 AND g.ejercicio = p.ejercicio and g.geografico = p.geografico and g.restrictiva='N' " + 
				"group by prestamo.ejercicio, prestamo.fuente, f.nombre, prestamo.organismo,  o.nombre, prestamo.correlativo, prestamo.prestamo_nombre, prestamo.prestamo_sigla, " + 
				"p.entidad,  e.NOMBRE, p.unidad_ejecutora,p.programa, prog.NOM_ESTRUCTURA, ue.nombre,p.subprograma, subp.NOM_ESTRUCTURA,p.proyecto, proy.NOM_ESTRUCTURA,p.obra,p.actividad, act.NOM_ESTRUCTURA " + 
				",p.renglon, r.nombre, p.geografico, g.nombre" ;
		boolean ret = false;		
		try{
			Connection conn = CHive.openConnection();			
			if(!conn.isClosed()&&CMemSQL.connect()){
				PreparedStatement pstm0 = conn.prepareStatement(query);
				PreparedStatement pstm = null;
				pstm0.setFetchSize(1000);
				ResultSet rs = pstm0.executeQuery();
				boolean first = true;
				int rows = 0;
				pstm = CMemSQL.getConnection().prepareStatement("Insert INTO prestamo (ejercicio,fuente,fuente_nombre,organismo,organismo_nombre,correlativo,prestamo_nombre,prestamo_sigla,entidad,entidad_nombre,unidad_ejecutora,"
						+ " unidad_ejecutora_nombre,programa,programa_nombre,subprograma,subprograma_nombre,proyecto,proyecto_nombre,actividad,obra,actividad_obra_nombre,renglon,renglon_nombre,geografico,geografico_nombre,"+
					       "asignado,vigente,ejecutado) "+
							"values (?,?,?,?,?,"
								  + "?,?,?,?,?,"
								  + "?,?,?,?,?,"
								  + "?,?,?,?,?,"
								  + "?,?,?,?,?,"
								  + "?,?,?) "); 
				while(rs.next()){	
					if (first){
						ret = true;	
						first=false;							
						PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("truncate table prestamo")  ;
						pstm2.executeUpdate();
						CLogger.writeConsole("Registros truncados");
						
						pstm2.close();
					}
					pstm.setInt(1, rs.getInt("ejercicio"));
					pstm.setInt(2, rs.getInt("fuente"));
					pstm.setString(3, rs.getString("fuente_nombre"));
					pstm.setInt(4, rs.getInt("organismo"));
					pstm.setString(5, rs.getString("organismo_nombre"));
					pstm.setInt(6, rs.getInt("correlativo"));
					pstm.setString(7, rs.getString("prestamo_nombre"));
					pstm.setString(8, rs.getString("prestamo_sigla"));
					pstm.setInt(9, rs.getInt("entidad"));
					pstm.setString(10, rs.getString("entidad_nombre"));
					pstm.setInt(11, rs.getInt("unidad_ejecutora"));
					pstm.setString(12, rs.getString("unidad_ejecutora_nombre"));
					pstm.setInt(13, rs.getInt("programa"));
					pstm.setString(14, rs.getString("programa_nombre"));
					pstm.setInt(15, rs.getInt("subprograma"));
					pstm.setString(16, rs.getString("subprograma_nombre"));
					pstm.setInt(17,rs.getInt("proyecto"));
					pstm.setString(18, rs.getString("proyecto_nombre"));
					pstm.setInt(19,rs.getInt("actividad"));
					pstm.setInt(20,rs.getInt("obra"));
					pstm.setString(21, rs.getString("actividad_obra_nombre"));
					pstm.setInt(22,rs.getInt("renglon"));
					pstm.setString(23, rs.getString("renglon_nombre"));
					pstm.setInt(24,rs.getInt("geografico"));
					pstm.setString(25, rs.getString("geografico_nombre"));
					pstm.setDouble(26, rs.getDouble("asignado"));
					pstm.setDouble(27, rs.getDouble("vigente"));
					pstm.setDouble(28, rs.getDouble("ejecutado"));
					pstm.addBatch();
					rows++;
					if((rows % 100) == 0){
						ret = ret & pstm.executeBatch().length>0;
						CLogger.writeConsole(String.join("Records escritos: ",String.valueOf(rows)));
					}
				}
				ret = ret && pstm.executeBatch().length>0;
				pstm.close();
				pstm0.close();
			}			
			conn.close();
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CEjecucionPrestamos.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}	
}
