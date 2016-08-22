package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CEjecucionCalamidad {
	
	public static boolean loadEjecucionFisicaFinanciera(boolean descentralizadas){
		DateTime date = new DateTime();
		String query = "select p.ejercicio,p.entidad, e.nombre entidad_nombre, p.unidad_ejecutora, ue.nombre unidad_ejecutora_nombre,  p.programa, prog.NOM_ESTRUCTURA programa_nombre "+
       ",p.subprograma, subp.nom_estructura subprograma_nombre,  p.proyecto, proy.nom_estructura proyecto_nombre, p.actividad, p.obra, act.nom_estructura actividad_nombre, "+
       "p.RENGLON, r.nombre renglon_nombre, metas.codigo_meta, metas.descripcion meta_nombre  , metas.unidad_medida, um.nombre unidad_medida_nombre, "+
       "case  when (metas.codigo_meta=1 or metas.codigo_meta is null) "+
       " then (sum(p.asignado) + sum(p.adicion) + sum(p.disminucion) + sum(p.traspaso_p)+ sum(p.traspaso_n)+ sum(p.transferencia_p) + sum(p.transferencia_n)) "+ 
       " else null "+
       "end vigente, "+
       "case when (metas.codigo_meta=1 or metas.codigo_meta is null) "+
       " then sum(gasto.monto_renglon) "+
       " else null "+
       "end ejecutado, metas.meta, avance.meta_avanzado "+
       "from EG_F6_PARTIDAS p  "+
       "left join (select distinct p3.entidad from eg_f6_partidas p3 where p3.unidad_ejecutora > 0 and p3.ejercicio = "+date.getYear()+") p2 on ( p2.entidad = p.entidad) "+ 
       "left join (select d.ejercicio, d.entidad, d.unidad_ejecutora, d.programa, d.subprograma, d.proyecto, d.obra, d.actividad, d.renglon, d.monto_renglon  "+
       "     from eg_gastos_hoja h, eg_gastos_detalle d  "+
       "     where h.ejercicio=d.ejercicio and h.entidad=d.entidad and h.unidad_ejecutora=d.unidad_ejecutora and h.no_cur=d.no_cur "+
       "     and h.estado='APROBADO' and h.clase_registro in ('DEV','CYD','REG','RDP') and d.ejercicio = "+date.getYear()+" and d.programa = 94 "+
       "   ) gasto "+
       "    on (p.ejercicio = gasto.ejercicio and p.entidad=gasto.entidad and p.unidad_ejecutora=gasto.unidad_ejecutora "+ 
       "       and p.programa=gasto.programa and p.subprograma=gasto.subprograma and p.proyecto=gasto.proyecto and p.obra=gasto.obra "+ 
       "       and p.actividad=gasto.actividad and p.renglon=gasto.renglon ) "+
       "left join (select m.ejercicio,m.entidad,m.unidad_ejecutora,m.programa,m.subprograma,m.proyecto,m.obra,m.actividad,m.codigo_meta,m.descripcion, m.unidad_medida,(m.CANTIDAD+m.ADICION+m.DISMINUCION) meta "+
       "     from sf_meta m left join (select distinct m3.entidad from sf_meta m3 where m3.unidad_ejecutora > 0 and m3.ejercicio = "+date.getYear()+") m2 on ( m2.entidad = m.entidad) "+
       "     where (m2.entidad is null or m.unidad_ejecutora>0) and m.ejercicio = "+date.getYear()+" and m.programa=94 "+
       "   ) metas "+
       "   on (p.ejercicio = metas.ejercicio and p.entidad=metas.entidad and p.unidad_ejecutora=metas.unidad_ejecutora "+
       "       and p.programa=metas.programa and p.subprograma = metas.subprograma and p.proyecto=metas.proyecto and p.obra=metas.obra "+
       "       and p.actividad=metas.actividad) "+
       "left join (select d.ejercicio, d.entidad, d.unidad_ejecutora, d.programa, d.subprograma, d.proyecto, d.obra, d.actividad, d.codigo_meta, d.cantidad_unidades meta_avanzado"+
       "     from sf_ejecucion_hoja_4 h, sf_ejecucion_detalle_4 d "+
       "     where h.ejercicio=d.ejercicio and h.entidad=d.entidad and h.unidad_ejecutora=d.unidad_ejecutora and h.no_cur=d.no_cur "+
       "     and h.estado='APROBADO' and d.ejercicio = "+date.getYear()+" and d.programa=94 "+
       "     ) avance   "+
       "     on(metas.ejercicio=avance.ejercicio and metas.entidad=avance.entidad and metas.unidad_ejecutora=avance.unidad_ejecutora "+
       "     and metas.programa=avance.programa and metas.subprograma=avance.subprograma and metas.proyecto=avance.proyecto and metas.obra=avance.obra "+
       "     and metas.actividad=avance.actividad and metas.codigo_meta=avance.codigo_meta) "+
       ",cg_entidades e, cg_entidades ue, cp_estructuras prog, cp_estructuras subp, cp_estructuras proy, cp_estructuras act, cp_objetos_gasto r, fp_unidad_medida um "+
       "where (p2.entidad is null or p.unidad_ejecutora>0)  "+
       "and p.ejercicio= "+date.getYear()+" and p.programa = 94  "+
       "and e.ejercicio=p.ejercicio and e.entidad=p.entidad and e.unidad_ejecutora = 0 "+
       "and ue.ejercicio=p.ejercicio and ue.entidad=p.entidad and ue.unidad_ejecutora=p.unidad_ejecutora "+
       "and prog.ejercicio=p.ejercicio and prog.entidad=p.entidad and prog.unidad_ejecutora=p.unidad_ejecutora and prog.programa=p.programa and prog.nivel_estructura=2 "+
       "and subp.ejercicio=p.ejercicio and subp.entidad=p.entidad and subp.unidad_ejecutora=p.unidad_ejecutora and subp.programa=p.programa and subp.subprograma=p.subprograma and subp.nivel_estructura=3 "+
       "and proy.ejercicio=p.ejercicio and proy.entidad=p.entidad and proy.unidad_ejecutora=p.unidad_ejecutora and proy.programa=p.programa and proy.subprograma=p.subprograma and proy.proyecto=p.proyecto and proy.nivel_estructura=4 "+
       "and act.ejercicio=p.ejercicio and act.entidad=p.entidad and act.unidad_ejecutora=p.unidad_ejecutora and act.programa=p.programa and act.subprograma=p.subprograma and act.proyecto=p.proyecto and act.obra=p.obra and act.actividad=p.actividad and act.nivel_estructura=5 "+
       "and r.ejercicio=p.ejercicio and r.renglon=p.renglon "+
       "and um.ejercicio=metas.ejercicio and um.codigo=metas.unidad_medida "+
       "group by p.ejercicio,p.entidad, e.nombre, p.unidad_ejecutora, ue.nombre,  p.programa, prog.nom_estructura, "+
       "p.subprograma, subp.nom_estructura, p.proyecto, proy.nom_estructura, p.actividad, p.obra, act.nom_estructura, "+
       "p.RENGLON, r.nombre, metas.codigo_meta, metas.descripcion  , metas.unidad_medida, um.nombre, "+
       "metas.meta,avance.meta_avanzado "+
       "order by p.ejercicio,p.entidad, p.unidad_ejecutora, p.programa,  p.subprograma, "+ 
       "p.proyecto, p.actividad, p.obra, p.RENGLON "+
       ",metas.codigo_meta";
		boolean ret = false;		
		try{
			Connection conn = descentralizadas ? CHive.openConnectiondes() : CHive.openConnection();			
			if(!conn.isClosed()&&CMemSQL.connect()){
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				if(rs!=null){
					int rows = 0;
					CLogger.writeConsole("CEjecucionCalamidad (loadEjecucionFisica "+ (descentralizadas ? "Descentralizadas" : "Centralizadas") +"):");
					PreparedStatement pstm;
					boolean first=true;
					ret=true;
					while(rs.next()){
						if (first && !descentralizadas){
							first=false;
							pstm = CMemSQL.getConnection().prepareStatement("delete from calamidad_ejecucion "
									+" where ejercicio=" + date.getYear())  ;
							if (pstm.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm.close();
						}
						pstm = CMemSQL.getConnection().prepareStatement("Insert INTO calamidad_ejecucion (ejercicio,entidad,entidad_nombre,unidad_ejecutora,"
								+ " unidad_ejecutora_nombre,programa,programa_nombre "+
							       ",subprograma,subprograma_nombre,proyecto,proyecto_nombre,actividad,obra,actividad_nombre, "+
							       "RENGLON,renglon_nombre,codigo_meta,meta_nombre,unidad_medida,unidad_medida_nombre, "+
							       "vigente, "+
							       "ejecutado,meta,meta_avanzado) "+
									"values (?,?,?,?,?,"
										  + "?,?,?,?,?,"
										  + "?,?,?,?,?,"
										  + "?,?,?,?,?,"
										  + "?,?,?,?) ");
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2, rs.getInt("entidad"));
						pstm.setString(3, rs.getString("entidad_nombre"));
						pstm.setInt(4, rs.getInt("unidad_ejecutora"));
						pstm.setString(5, rs.getString("unidad_ejecutora_nombre"));
						pstm.setInt(6, rs.getInt("programa"));
						pstm.setString(7, rs.getString("programa_nombre"));
						pstm.setInt(8, rs.getInt("subprograma"));
						pstm.setString(9, rs.getString("subprograma_nombre"));
						pstm.setInt(10,rs.getInt("proyecto"));
						pstm.setString(11, rs.getString("proyecto_nombre"));
						pstm.setInt(12,rs.getInt("actividad"));
						pstm.setInt(13,rs.getInt("obra"));
						pstm.setString(14, rs.getString("actividad_nombre"));
						pstm.setInt(15,rs.getInt("renglon"));
						pstm.setString(16, rs.getString("renglon_nombre"));
						pstm.setInt(17,rs.getInt("codigo_meta"));
						pstm.setString(18, rs.getString("meta_nombre"));
						pstm.setInt(19,rs.getInt("unidad_medida"));
						pstm.setString(20, rs.getString("unidad_medida_nombre"));
						pstm.setDouble(21, rs.getDouble("vigente"));
						pstm.setDouble(22, rs.getDouble("ejecutado"));
						pstm.setDouble(23, rs.getDouble("meta"));
						pstm.setDouble(24, rs.getDouble("meta_avanzado"));
						ret = ret && pstm.executeUpdate()>0;
						rows++;
						if((rows % 5) == 0)
							CLogger.writeConsole(String.join("Records escritos: ",String.valueOf(rows)));
						pstm.close();
					}		
				}
			}
			conn.close();
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CEjecucionCalamidad.class", e);
		}
		finally{
			CMemSQL.close();
			ret=true;
		}
		return ret;
	}	
	
	public static boolean loadActividadesPresupuestarias(boolean descentralizadas,int programa){
		boolean ret=false;
		int act = 0;
		DateTime date = new DateTime();
		try{
			Connection conn = descentralizadas ? CHive.openConnectiondes() : CHive.openConnection();			
			if(!conn.isClosed()&&CMemSQL.connect()){
				ResultSet rs = conn.prepareStatement("select e1.ejercicio,e1.ENTIDAD,e1.unidad_ejecutora,e1.programa,e1.subprograma,e1.proyecto,e1.obra,e1.actividad, " + 
						"e1.nom_estructura nombre,e1.DESCRIPCION, meta.meta,meta.fecha_inicio,meta.fecha_fin,ejecucion.avance " + 
						"from sicoinprod.cp_estructuras e1 " + 
						"  left join (  " + 
						"  select distinct e3.entidad from sicoinprod.cp_estructuras e3 where e3.unidad_ejecutora > 0 and e3.ejercicio = "+ date.getYear()+" " + 
						"  ) e2  " + 
						"  on (  " + 
						"    e2.entidad = e1.entidad " + 
						"  )  " + 
						"  left join ( " + 
						"  select  m.ejercicio,m.entidad,m.unidad_ejecutora,m.programa,m.subprograma,m.proyecto,m.actividad,m.obra, " + 
						"         sum(m.cantidad+m.adicion+m.disminucion) meta, min(m.fecha_inicio) fecha_inicio, max(m.fecha_fin) fecha_fin           " + 
						"  from SF_META m  " + 
						"  group by  m.ejercicio,m.entidad,m.unidad_ejecutora,m.programa,m.subprograma,m.proyecto,m.actividad,m.obra " + 
						") meta " + 
						"on ( " + 
						"  meta.ejercicio=e1.ejercicio " + 
						"  and meta.entidad=e1.entidad " + 
						"  and meta.unidad_ejecutora=e1.unidad_ejecutora " + 
						"  and meta.programa=e1.programa " + 
						"  and meta.subprograma=e1.subprograma " + 
						"  and meta.proyecto=e1.proyecto " + 
						"  and meta.obra=e1.obra " + 
						"  and meta.actividad=e1.actividad " + 
						")  " + 
						"left join ( " + 
						"  select d.ejercicio,d.entidad,d.unidad_ejecutora,d.programa,d.subprograma,d.proyecto,d.obra,d.actividad,sum(d.CANTIDAD_UNIDADES) avance " + 
						"  from sicoinprod.sf_ejecucion_hoja_4 h, sicoinprod.sf_ejecucion_detalle_4 d " + 
						"  where h.ejercicio = d.ejercicio " + 
						"  and h.entidad = d.entidad " + 
						"  and h.unidad_ejecutora = d.unidad_ejecutora " + 
						"  and h.no_cur = d.no_cur " + 
						"  and h.estado='APROBADO' " + 
						"  group by d.ejercicio,d.entidad,d.unidad_ejecutora,d.programa,d.subprograma,d.proyecto,d.obra,d.actividad " + 
						") ejecucion  " + 
						"  on ( " + 
						"    e1.ejercicio=ejecucion.ejercicio " + 
						"    and e1.entidad=ejecucion.entidad " + 
						"    and e1.unidad_ejecutora = ejecucion.unidad_ejecutora " + 
						"    and e1.programa = ejecucion.programa " + 
						"    and e1.subprograma = ejecucion.subprograma " + 
						"    and e1.proyecto = ejecucion.proyecto " + 
						"    and e1.obra = ejecucion.obra " + 
						"    and e1.actividad = ejecucion.actividad " + 
						"  )  " + 
						"  where e1.programa="+programa+" " + 
						"  and e1.ejercicio="+date.getYear()+"  " + 
						"  and e1.nivel_estructura=5 " + 
						"  and (e2.entidad is null or e1.unidad_ejecutora>0) ").executeQuery();
				ResultSet rs2;
				PreparedStatement pstm,pstm2;
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				while(rs.next()){
					pstm = CMemSQL.getConnection().prepareStatement("select id from seg_actividad where ejercicio=? and entidad=? and unidad_ejecutora=? "
							+ "and programa=? "
							+ "and subprograma=? "
							+ "and proyecto=? "
							+ "and actividad=? "
							+ "and obra=? ");
					pstm.setInt(1, rs.getInt("ejercicio"));
					pstm.setInt(2, rs.getInt("entidad"));
					pstm.setInt(3, rs.getInt("unidad_ejecutora"));
					pstm.setInt(4, rs.getInt("programa"));
					pstm.setInt(5, rs.getInt("subprograma"));
					pstm.setInt(6, rs.getInt("proyecto"));
					pstm.setInt(7, rs.getInt("actividad"));
					pstm.setInt(8, rs.getInt("obra"));
					rs2 = pstm.executeQuery();
					if (rs2.next()){
						pstm2 =CMemSQL.getConnection().prepareStatement("update seg_actividad set porcentaje_ejecucion = ?, usuario_actualizacion='SICOIN', fecha_actualizacion=? where id=?");
						if (rs.getDouble("meta")>0 && rs.getDouble("avance")>0)
							pstm2.setDouble(1, (rs.getDouble("avance")/rs.getDouble("meta"))*100);
						else
							pstm2.setDouble(1, 0.0);
						pstm2.setTimestamp(2, new Timestamp(date.getMillis()));
						pstm2.setInt(3, rs2.getInt("id"));
					}else{
						pstm2 = CMemSQL.getConnection().prepareStatement("insert into seg_actividad (nombre,descripcion,fecha_inicio,fecha_fin,porcentaje_ejecucion,"
								+ "ejercicio,entidad,unidad_ejecutora,programa,subprograma,proyecto,"
								+ "actividad,obra,fecha_creacion,usuario_creacion) "
								+ "values 	(?,?,?,?,?,"
								+ 			"?,?,?,?,?,"
								+ 			"?,?,?,?,?)");
						pstm2.setString(1, rs.getString("nombre"));
						pstm2.setString(2, rs.getString("descripcion"));
						
						if (rs.getString("fecha_inicio")!=null)
							pstm2.setTimestamp(3,new java.sql.Timestamp(dateFormat.parse(rs.getString("fecha_inicio")).getTime()));
						else 
							pstm2.setNull(3, java.sql.Types.TIMESTAMP);
						
						if (rs.getString("fecha_fin")!=null)
							pstm2.setTimestamp(4,new java.sql.Timestamp(dateFormat.parse(rs.getString("fecha_fin")).getTime()));
						else
							pstm2.setNull(4, java.sql.Types.TIMESTAMP);
						if (rs.getDouble("meta")>0 && rs.getDouble("avance")>0)
							pstm2.setDouble(5, (rs.getDouble("avance")/rs.getDouble("meta"))*100);
						else 
							pstm2.setNull(5, java.sql.Types.DOUBLE);
						pstm2.setInt(6,rs.getInt("ejercicio"));
						pstm2.setInt(7, rs.getInt("entidad"));
						pstm2.setInt(8, rs.getInt("unidad_ejecutora"));
						pstm2.setInt(9, rs.getInt("programa"));
						pstm2.setInt(10, rs.getInt("subprograma"));
						pstm2.setInt(11, rs.getInt("proyecto"));
						pstm2.setInt(12, rs.getInt("actividad"));
						pstm2.setInt(13, rs.getInt("obra"));
						pstm2.setTimestamp(14, new Timestamp(date.getMillis()));
						pstm2.setString(15, "SICOIN");
					}
					act = pstm2.executeUpdate();
					pstm.close();
					rs2.close();
				}
			}
			conn.close();
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 2: CEjecucionCalamidad.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret&act>0;
	}
}
