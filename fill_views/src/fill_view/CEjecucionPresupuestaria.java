package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import utilities.CLogger;

public class CEjecucionPresupuestaria {
	
	public static boolean loadEjecucionPresupuestaria(Connection conn, Integer ejercicio){
		
		boolean ret = false;
		try{
			if( !conn.isClosed() && CMemSQL.connect()){
				ret = true;

				CLogger.writeConsole("CEjecucionPresupuestaria Entidades (Ejercicio "+ejercicio+"):");
				CLogger.writeConsole("Eliminando data actual:");
				PreparedStatement pstm;
				List<String> tablas = Arrays.asList("mv_entidad", "mv_estructura", "mv_cuota","mv_gasto", "mv_anticipo","mv_vigente", "mv_ejecucion_presupuestaria","mv_ejecucion_presupuestaria_geografico","mv_ejecucion_presupuestaria_mensualizada");
				
				CLogger.writeConsole("Copiando historia:");
				for(String tabla:tablas){
					CLogger.writeConsole("Copiando historia - "+tabla);
					pstm = conn.prepareStatement("TRUNCATE TABLE dashboard."+tabla);
					pstm.executeUpdate();
					pstm.close();
					pstm = conn.prepareStatement("INSERT INTO dashboard."+tabla+" SELECT * FROM dashboard_historia."+tabla+" where ejercicio < ?");
					pstm.setInt(1, ejercicio);
					pstm.executeUpdate();
					pstm.close();
				}
				
				CLogger.writeConsole("Insertando valores a MV_ENTIDAD");
				pstm =conn.prepareStatement("insert into table dashboard.mv_entidad "+
						"select ejercicio, entidad, count(*) unidades_ejecutoras " + 
						"from sicoinprod.cg_entidades " + 
						"where ejercicio= ? "+
						"group by ejercicio, entidad");
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_ESTRUCTURA");
				pstm = conn.prepareStatement("insert into table dashboard.mv_estructura "+
						"select e.ejercicio, e.entidad, e.nombre entidad_nombre, es.sigla, ue.unidad_ejecutora, ue.nombre unidad_ejecutora_nombre,  " + 
						"p.programa, p.nom_estructura programa_nombre,  " + 
						"sp.subprograma, sp.nom_estructura subprograma_nombre,  " + 
						"pr.proyecto, pr.nom_estructura proyecto_nombre,  " + 
						"ao.actividad, ao.obra, ao.nom_estructura actividad_obra_nombre  " + 
						"from sicoinprod.cg_entidades e, sicoinprod.cg_entidades ue " + 
						"left outer join sicoinprod.cp_estructuras p  " + 
						"on( " + 
						"p.ejercicio = ue.ejercicio  " + 
						"and p.entidad = ue.entidad  " + 
						"and p.unidad_ejecutora = ue.unidad_ejecutora  " + 
						"and p.nivel_estructura = 2 " + 
						") left outer join sicoinprod.cp_estructuras sp  " + 
						"on( " + 
						"sp.ejercicio = ue.ejercicio  " + 
						"and sp.entidad = ue.entidad  " + 
						"and sp.unidad_ejecutora = ue.unidad_ejecutora  " + 
						"and sp.programa = p.programa  " + 
						"and sp.nivel_estructura = 3  " + 
						") left outer join sicoinprod.cp_estructuras pr " + 
						"on( " + 
						"pr.ejercicio = ue.ejercicio  " + 
						"and pr.entidad = ue.entidad   " + 
						"and pr.unidad_ejecutora = ue.unidad_ejecutora  " + 
						"and pr.programa = p.programa  " + 
						"and pr.subprograma = sp.subprograma  " + 
						"and pr.nivel_estructura = 4  " + 
						") left outer join sicoinprod.cp_estructuras ao " + 
						"on( " + 
						"ao.ejercicio = ue.ejercicio  " + 
						"and ao.entidad = ue.entidad  " + 
						"and ao.unidad_ejecutora = ue.unidad_ejecutora  " + 
						"and ao.programa = p.programa  " + 
						"and ao.subprograma = sp.subprograma  " + 
						"and ao.proyecto = pr.proyecto  " + 
						"and ao.nivel_estructura = 5 " + 
						"), dashboard.mv_entidad mve, dashboard.entidad_sigla es  " + 
						"where e.ejercicio = ?  " + 
						"and ue.ejercicio = e.ejercicio  " + 
						"and e.restrictiva = 'N'  " + 
						"and ue.restrictiva = 'N'  " + 
						"and e.unidad_ejecutora = 0  " + 
						"and ue.entidad = e.entidad  " + 
						"and ((e.entidad between 11130000 and 11130020) OR e.entidad = 11140021)   " + 
						"and mve.entidad = e.entidad  " + 
						"and mve.ejercicio = e.ejercicio  " + 
						"and ((ue.ejercicio=mve.ejercicio and ue.unidad_ejecutora = 0 and mve.unidades_ejecutoras=1) or (ue.ejercicio=mve.ejercicio and ue.unidad_ejecutora>0 and mve.unidades_ejecutoras>1))  " + 
						"and es.entidad = e.entidad");
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_GASTO");
				///Actualiza la vista de gasto
				pstm = conn.prepareStatement("insert into table dashboard.mv_gasto "
						+"select "+ejercicio+" ejercicio,month(gh.fec_aprobado) mes, gd.entidad, gd.unidad_ejecutora, gd.programa, gd.subprograma, gd.proyecto, gd.actividad, gd.obra, gd.renglon, r.nombre renglon_nombre, gd.fuente,     " + 
						"						 						 	gd.renglon - (gd.renglon%100) grupo, gg.nombre grupo_nombre, gd.renglon - (gd.renglon%10) subgrupo, sg.nombre subgrupo_nombre, gd.geografico,     " + 
						"						 						 	sum( case when gh.ejercicio = (? - 5) then gd.monto_renglon else 0 end) ano_1,      " + 
						"						 						 	sum( case when gh.ejercicio = (? - 4) then gd.monto_renglon else 0 end) ano_2,      " + 
						"						 						 	sum( case when gh.ejercicio = (? - 3) then gd.monto_renglon else 0 end) ano_3,      " + 
						"						 						 	sum( case when gh.ejercicio = (? - 2) then gd.monto_renglon else 0 end) ano_4,      " + 
						"						 						 	sum( case when gh.ejercicio = (? - 1) then gd.monto_renglon else 0 end) ano_5,      " + 
						"						 						 	sum( case when gh.ejercicio = ? then gd.monto_renglon else 0 end) ano_actual      " + 
						"						 						 				from sicoinprod.eg_gastos_hoja gh, sicoinprod.eg_gastos_detalle gd,      " + 
						"						 										sicoinprod.cp_grupos_gasto gg, sicoinprod.cp_objetos_gasto sg, sicoinprod.cp_objetos_gasto r  		 " + 
						"						 						 				where gh.ejercicio = gd.ejercicio         " + 
						"						 						 				and gh.entidad = gd.entidad       " + 
						"						 						 				and gh.unidad_ejecutora = gd.unidad_ejecutora       " + 
						"						 						 				and gh.no_cur = gd.no_cur       " + 
						"						 						 				and gh.clase_registro IN ('DEV', 'CYD', 'RDP', 'REG')       " + 
						"						 						 				and gh.estado = 'APROBADO'       " + 
						"						 						 				and gh.ejercicio > ( ? - 6 )       " + 
						"						  										and gg.ejercicio =  ? " + 
						"		 				  										and gg.grupo_gasto = (gd.renglon-(gd.renglon%100))   " + 
						"		 				  										and sg.ejercicio = ?    " + 
						"		 				  										and sg.renglon = (gd.renglon - (gd.renglon%10))        " + 
						"		 				  										and r.ejercicio = ?   " + 
						"		 				  										and r.renglon = gd.renglon   " + 
						"						 						 				group by month(gh.fec_aprobado), gd.entidad, gd.unidad_ejecutora, gd.programa, gd.subprograma,       " + 
						"						 						 				gd.proyecto, gd.actividad, gd.obra, gg.nombre, sg.nombre, r.nombre, gd.renglon, gd.fuente, gd.geografico  ");
				pstm.setInt(1, ejercicio);
				pstm.setInt(2, ejercicio);
				pstm.setInt(3, ejercicio);
				pstm.setInt(4, ejercicio);
				pstm.setInt(5, ejercicio);
				pstm.setInt(6, ejercicio);
				pstm.setInt(7, ejercicio);
				pstm.setInt(8, ejercicio);
				pstm.setInt(9, ejercicio);
				pstm.setInt(10, ejercicio);
 				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_CUOTA");
				///Actualiza la vista de cuota
				pstm = conn.prepareStatement("INSERT INTO table dashboard.mv_cuota "+
						"SELECT d.ejercicio, t.mes,  " + 
						"                   d.entidad,  " + 
						"                   d.unidad_ejecutora,  " + 
						"                   d.fuente,   " + 
						"                   sum(case  " + 
						"                   when t.mes = 1 and d.cuatrimestre = 1 then d.cuota_mes1_sol  " + 
						"                   when t.mes = 2 and d.cuatrimestre = 1 then d.cuota_mes2_sol " + 
						"                   when t.mes = 3 and d.cuatrimestre = 1 then d.cuota_mes3_sol " + 
						"                   when t.mes = 4 and d.cuatrimestre = 1 then d.cuota_mes4_sol " + 
						"                   when t.mes = 5 and d.cuatrimestre = 2 then d.cuota_mes1_sol  " + 
						"                   when t.mes = 6 and d.cuatrimestre = 2 then d.cuota_mes2_sol " + 
						"                   when t.mes = 7 and d.cuatrimestre = 2 then d.cuota_mes3_sol " + 
						"                   when t.mes = 8 and d.cuatrimestre = 2 then d.cuota_mes4_sol " + 
						"                   when t.mes = 9 and d.cuatrimestre = 3 then d.cuota_mes1_sol  " + 
						"                   when t.mes = 10 and d.cuatrimestre = 3 then d.cuota_mes2_sol " + 
						"                   when t.mes = 11 and d.cuatrimestre = 3 then d.cuota_mes3_sol " + 
						"                   when t.mes = 12 and d.cuatrimestre = 3 then d.cuota_mes4_sol " + 
						"                   end ) solicitado, " + 
						"                   sum(case  " + 
						"                   when t.mes = 1 and d.cuatrimestre = 1 then d.cuota_mes1_apr " + 
						"                   when t.mes = 2 and d.cuatrimestre = 1 then d.cuota_mes2_apr " + 
						"                   when t.mes = 3 and d.cuatrimestre = 1 then d.cuota_mes3_apr " + 
						"                   when t.mes = 4 and d.cuatrimestre = 1 then d.cuota_mes4_apr " + 
						"                   when t.mes = 5 and d.cuatrimestre = 2 then d.cuota_mes1_apr " + 
						"                   when t.mes = 6 and d.cuatrimestre = 2 then d.cuota_mes2_apr " + 
						"                   when t.mes = 7 and d.cuatrimestre = 2 then d.cuota_mes3_apr " + 
						"                   when t.mes = 8 and d.cuatrimestre = 2 then d.cuota_mes4_apr " + 
						"                   when t.mes = 9 and d.cuatrimestre = 3 then d.cuota_mes1_apr " + 
						"                   when t.mes = 10 and d.cuatrimestre = 3 then d.cuota_mes2_apr " + 
						"                   when t.mes = 11 and d.cuatrimestre = 3 then d.cuota_mes3_apr " + 
						"                   when t.mes = 12 and d.cuatrimestre = 3 then d.cuota_mes4_apr " + 
						"                   end ) aprobado " + 
						"                   FROM sicoinprod.EG_FINANCIERO_DETALLE_4 D,  " + 
						"                   sicoinprod.eg_financiero_hoja_4 H1, dashboard.tiempo t  " + 
						"                   WHERE  h1.ejercicio = ?   " + 
						"                   and h1.ejercicio = d.ejercicio " + 
						"                    and t.ejercicio = h1.ejercicio " + 
						"                    and t.dia = 1 " + 
						"                    AND h1.entidad = d.entidad  " + 
						"                    AND h1.unidad_ejecutora = d.unidad_ejecutora  " + 
						"                    AND h1.unidad_desconcentrada = d.unidad_desconcentrada  " + 
						"                    AND h1.no_cur = d.no_cur  " + 
						"                    AND H1.CLASE_REGISTRO IN ('RPG', 'PRG', 'RPGI')  " + 
						"                    AND H1.estado = 'APROBADO'  " + 
						"                    GROUP BY d.ejercicio,  " + 
						"                    t.mes, " + 
						"                    d.entidad,  " + 
						"                    d.unidad_ejecutora,  " + 
						"                    d.fuente "
						);	
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_ANTICIPO");
				///Actualiza la vista de cuota
				pstm = conn.prepareStatement("INSERT INTO table dashboard.mv_anticipo "+
						"SELECT d.ejercicio, " + 
						" 		t.mes,   " + 
						"        d.entidad,   " + 
						"        d.unidad_ejecutora,   " + 
						"        d.fuente,   " + 
						"        sum(d.cuota_fondo_rotativo_apr + d.cuota_fideicomisos_apr + d.CUOTA_CONVENIOS_APR + d.cuota_contratos_apr + d.cuota_otros_apr + d.cuota_paa_apr) anticipo   " + 
						"	    FROM  sicoinprod.eg_anticipo_hoja h,   " + 
						"	          sicoinprod.eg_anticipo_detalle d, " + 
						"	          dashboard.tiempo t   " + 
						"	    WHERE h.ejercicio = d.ejercicio   " + 
						"	    AND h.ejercicio= ? " + 
						"	    and t.ejercicio = h.ejercicio " + 
						"	    and t.dia = 1 " + 
						"	    and d.cuatrimestre = ceil(t.mes/4) " + 
						"	    and d.mes = t.mes-((d.cuatrimestre-1)*4) " + 
						"	    AND   h.entidad = d.entidad   " + 
						"	    AND   h.unidad_ejecutora = d.unidad_ejecutora   " + 
						"	    AND   h.no_cur = d.no_cur   " + 
						"	    AND   h.estado = 'APROBADO'   " + 
						"	    AND   h.clase_registro IN ('PRG','RPG')   " + 
						"	    group by d.ejercicio, t.mes, d.entidad, d.unidad_ejecutora, d.fuente"
						);	
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_VIGENTE");
				//Actualiza la vista de mv_vigente
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard.mv_vigente " + 
						"select asignado.ejercicio, asignado.mes, asignado.entidad, asignado.unidad_ejecutora, asignado.programa, asignado.subprograma, asignado.proyecto, asignado.actividad, asignado.obra,    " + 
						"						asignado.fuente, asignado.grupo, gg.nombre grupo_nombre, asignado.subgrupo,  sg.nombre subgrupo_nombre, " + 
						"						asignado.renglon, r.nombre renglon_nombre, asignado.geografico,   " + 
						"						sum(asignado.asignado) asignado, sum(asignado.asignado)+nvl(sum(vigente.modificaciones),0) vigente   " + 
						"						from (   " + 
						"							select t.ejercicio,      " + 
						"													t.mes,      " + 
						"													a.entidad,      " + 
						"													a.unidad_ejecutora,      " + 
						"													a.programa,      " + 
						"													a.subprograma,      " + 
						"													a.proyecto,      " + 
						"													a.actividad,      " + 
						"													a.obra,      " + 
						"													a.fuente,      " + 
						"													(a.renglon-a.renglon%100) grupo,      " + 
						"													(a.renglon-a.renglon%10) subgrupo,      " + 
						"													a.renglon,      " + 
						"													a.geografico,      " + 
						"													sum(a.asignado) asignado      " + 
						"													from dashboard.tiempo t      " + 
						"													left outer join      " + 
						"													(select a.ejercicio, a.entidad, a.unidad_ejecutora, a.programa, a.subprograma, a.proyecto, a.actividad, a.obra, a.fuente,      " + 
						"														a.renglon, a.geografico, a.asignado      " + 
						"														from sicoinprod.eg_f6_partidas a) a on (a.ejercicio = t.ejercicio and t.dia=1 and t.ejercicio=?)    " + 
						"														left outer join (   " + 
						"															select ejercicio, entidad, count(*) ues    " + 
						"															from sicoinprod.cg_entidades   " + 
						"															group by ejercicio, entidad ) ues on ( ues.ejercicio = t.ejercicio and ues.entidad = a.entidad)   " + 
						"													where ((a.unidad_ejecutora>0 and ues.ues>0) OR (a.unidad_ejecutora=0 and ues.ues=1))   " + 
						"													group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14 " + 
						"								 " + 
						"						) asignado left outer join   " + 
						"						(   " + 
						"						select t.ejercicio, t.mes, m.entidad, m.unidad_ejecutora, m.programa, m.subprograma, m.proyecto, m.actividad, m.obra, m.fuente, m.renglon, m.geografico,  " + 
						"						sum(case when m.mes<=t.mes then m.modificaciones else 0 end) modificaciones " + 
						"						from dashboard.tiempo t left outer join ( " + 
						"							select mh.ejercicio, month(mh.fec_aprobado) mes, md.entidad, md.unidad_ejecutora, md.programa, md.subprograma, md.proyecto, md.actividad, md.obra, md.fuente, md.renglon,   " + 
						"							md.geografico,   " + 
						"							sum(md.monto_aprobado) modificaciones   " + 
						"								from sicoinprod.eg_modificaciones_hoja mh left outer join    " + 
						"								dashboard.mv_entidad num_ues on (mh.entidad=num_ues.entidad and mh.ejercicio = num_ues.ejercicio), sicoinprod.eg_modificaciones_detalle md   " + 
						"								where md.ejercicio = mh.ejercicio   " + 
						"								and num_ues.ejercicio = mh.ejercicio   " + 
						"								and num_ues.entidad = mh.entidad   " + 
						"								and md.clase_registro = mh.clase_registro   " + 
						"								and md.no_cur = mh.no_cur   " + 
						"								and md.unidad_ejecutora = mh.unidad_ejecutora   " + 
						"								and md.entidad = mh.entidad   " + 
						"								and mh.APROBADO = 'S'   " + 
						"								and ((mh.unidad_ejecutora>0 and num_ues.unidades_ejecutoras>0) OR (mh.unidad_ejecutora=0 and num_ues.unidades_ejecutoras=1))   " + 
						"								and md.ejercicio = mh.ejercicio " + 
						"								group by mh.ejercicio, month(mh.fec_aprobado), md.mes_modificacion, md.entidad,  md.unidad_ejecutora,  md.programa,  md.subprograma,  md.proyecto,  md.actividad,  md.obra,     " + 
						"								md.renglon, md.fuente, md.geografico  " + 
						"							) m on (t.ejercicio = m.ejercicio and t.dia=1 and t.ejercicio = ?)  " + 
						"							group by t.ejercicio, t.mes, m.entidad, m.unidad_ejecutora, m.programa, m.subprograma, m.proyecto, m.actividad, m.obra, m.fuente, m.renglon, m.geografico  " + 
						"						) vigente   " + 
						"						on (asignado.ejercicio = vigente.ejercicio   " + 
						"						and asignado.mes = vigente.mes    " + 
						"						and asignado.entidad = vigente.entidad   " + 
						"						and asignado.unidad_ejecutora = vigente.unidad_ejecutora    " + 
						"						and asignado.programa = vigente.programa   " + 
						"						and asignado.subprograma = vigente.subprograma   " + 
						"						and asignado.proyecto = vigente.proyecto   " + 
						"						and asignado.actividad = vigente.actividad   " + 
						"						and asignado.obra = vigente.obra   " + 
						"						and asignado.fuente = vigente.fuente " + 
						"						and asignado.renglon = vigente.renglon   " + 
						"						and asignado.geografico = vigente.geografico), sicoinprod.cp_grupos_gasto gg, sicoinprod.cp_objetos_gasto sg, sicoinprod.cp_objetos_gasto r " + 
						"						where gg.grupo_gasto = asignado.grupo  " + 
						"						and gg.ejercicio = asignado.ejercicio    " + 
						"						and sg.renglon = asignado.subgrupo   " + 
						"						and sg.ejercicio = asignado.ejercicio " + 
						"						and r.ejercicio = asignado.ejercicio   " + 
						"						and r.renglon = asignado.renglon " + 
						"						group by asignado.ejercicio, asignado.mes, asignado.entidad, asignado.unidad_ejecutora, asignado.programa, asignado.subprograma, asignado.proyecto, asignado.actividad, asignado.obra,    " + 
						"						asignado.fuente, asignado.grupo, gg.nombre, asignado.subgrupo, sg.nombre, asignado.renglon, r.nombre, asignado.geografico  ");
				pstm.setInt(1, ejercicio);
				pstm.setInt(2, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_EJECUCION_PRESUPUESTARIA");
				//Actualiza la vista de mv_ejecucion_presupuestaria
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard.mv_ejecucion_presupuestaria  "+
						"select    " + 
						"												t1.ejercicio,    " + 
						"												t1.entidad,    " + 
						"												t1.unidad_ejecutora,    " + 
						"												t1.programa,    " + 
						"												t1.subprograma,    " + 
						"												t1.proyecto,    " + 
						"												t1.actividad,    " + 
						"												t1.obra,    " + 
						"												t1.mes, t1.fuente, t1.grupo, t1.grupo_nombre, t1.subgrupo, t1.subgrupo_nombre, t1.renglon, t1.renglon_nombre,     " + 
						"																		 t1.ano_1, t1.ano_2, t1.ano_3, t1.ano_4, t1.ano_5, t1.ano_actual, t1.asignado, t1.vigente, a.anticipo anticipo_cuota, c.solicitado solicitado_cuota,      " + 
						"																		 c.aprobado aprobado_cuota       " + 
						"																		 from (      " + 
						"																		 	select " +
						"																			nvl(v.ejercicio,g.ejercicio) ejercicio, "+	
						"																			nvl(v.mes,g.mes) mes,       " + 
						"																		 	nvl(v.entidad,g.entidad) entidad,      " + 
						"																		 	nvl(v.unidad_ejecutora, g.unidad_ejecutora) unidad_ejecutora,      " + 
						"																		 	nvl(v.programa,g.programa) programa,      " + 
						"																		 	nvl(v.subprograma, g.subprograma) subprograma,      " + 
						"																		 	nvl(v.proyecto, g.proyecto) proyecto,      " + 
						"																		 	nvl(v.actividad, g.actividad) actividad,      " + 
						"																		 	nvl(v.obra, g.obra) obra,      " + 
						"																		 	nvl(v.fuente, g.fuente) fuente,      " + 
						"																		 	nvl(v.grupo, g.grupo) grupo,      " + 
						"																		 	nvl(v.grupo_nombre, g.grupo_nombre) grupo_nombre,      " + 
						"																		 	nvl(v.subgrupo, g.subgrupo) subgrupo,      " + 
						"																		 	nvl(v.subgrupo_nombre, g.subgrupo_nombre) subgrupo_nombre,      " + 
						"																		 	nvl(v.renglon, g.renglon) renglon,      " + 
						"																		 	nvl(v.renglon_nombre, g.renglon_nombre) renglon_nombre,      " + 
						"																			g.ano_1 ano_1, g.ano_2 ano_2, g.ano_3 ano_3, g.ano_4 ano_4, g.ano_5 ano_5, g.ano_actual ano_actual,      " + 
						"																		 	v.asignado asignado, v.vigente vigente      " + 
						"																		 	from  (	select ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, grupo, grupo_nombre, " + 
						"								    										   subgrupo, subgrupo_nombre, renglon, renglon_nombre, fuente, sum(asignado) asignado, sum(vigente) vigente " + 
						"																			   from dashboard.mv_vigente " + 
						"																			   where ejercicio = ? " + 
						"																			   group by ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, grupo, grupo_nombre, subgrupo, subgrupo_nombre, renglon, renglon_nombre, fuente " + 
						"																			) v full outer join   (" + 
						"																		 		select g1.ejercicio, g1.mes, g1.entidad, g1.unidad_ejecutora, g1.programa, g1.subprograma, g1.proyecto, g1.actividad, g1.obra, g1.fuente," + 
						"																		 		g1.grupo, g1.grupo_nombre, g1.subgrupo, g1.subgrupo_nombre, g1.renglon, g1.renglon_nombre," + 
						"																		 		sum(g1.ano_1) ano_1, sum(g1.ano_2) ano_2, sum(g1.ano_3) ano_3, sum(g1.ano_4) ano_4, sum(g1.ano_5) ano_5," + 
						"																		 		sum(g1.ano_actual) ano_actual" + 
						"																		 		from dashboard.mv_gasto g1 " +
						"																				where g1.ejercicio = ? " +		
						"																		 		group by g1.ejercicio, g1.mes, g1.entidad, g1.unidad_ejecutora, g1.programa, g1.subprograma, g1.proyecto, g1.actividad, g1.obra, g1.fuente, " + 
						"																		 		g1.grupo, g1.grupo_nombre, g1.subgrupo, g1.subgrupo_nombre, g1.renglon, g1.renglon_nombre" + 
						"																		 	) g "+
						"																		 	on(       " + 
						"																		 		g.entidad = v.entidad      " + 
						"																		 		and g.unidad_ejecutora = v.unidad_ejecutora      " + 
						"																		 		and g.programa = v.programa      " + 
						"																		 		and g.subprograma = v.subprograma      " + 
						"																		 		and g.proyecto = v.proyecto      " + 
						"																		 		and g.actividad = v.actividad      " + 
						"																		 		and g.obra = v.obra      " + 
						"																		 		and g.renglon = v.renglon      " + 
						"																		 		and g.fuente = v.fuente      " + 
						"																		 		and g.mes = v.mes      " + 
						"																				and g.ejercicio = v.ejercicio " +
						"																		 	  )    " + 
						"																		 ) t1    " + 
						"																		 left outer join dashboard.mv_cuota c   " + 
						"																		 on (   " + 
						" 																			c.ejercicio = t1.ejercicio " +
						"																		 	and c.entidad = t1.entidad      " + 
						"																		 	and c.unidad_ejecutora = t1.unidad_ejecutora      " + 
						"																		 	and c.fuente = t1.fuente      " + 
						"																		 	and c.mes = t1.mes   " + 
						"																		 )   " + 
						"																		 left outer join dashboard.mv_anticipo a  " + 
						"																		 on(  " + 
						"																			a.ejercicio = t1.ejercicio " +
						"																		 	and a.entidad = t1.entidad  " + 
						"																		 	and a.unidad_ejecutora = t1.unidad_ejecutora  " + 
						"																		 	and a.fuente = t1.fuente  " + 
						"																		 	and a.mes = t1.mes  " + 
						"																		 ) " );
				pstm.setInt(1, ejercicio);
				pstm.setInt(2, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_EJECUCION_PRESUPUESTARIA_GEOGRAFICO");
				//Actualiza la vista de mv_ejecucion_presupuestaria
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard.mv_ejecucion_presupuestaria_geografico "+
						"select " + 
						"v.ejercicio, v.mes,  " + 
						"v.entidad,  " + 
						"v.unidad_ejecutora,  " + 
						"v.programa,  " + 
						"v.subprograma,  " + 
						"v.proyecto,  " + 
						"v.actividad,  " + 
						"v.obra,  " + 
						"v.fuente,     " + 
						"v.grupo,  " + 
						"v.subgrupo,  " + 
						"v.renglon,  " + 
						"v.geografico,   " + 
						"sum(g.ano_actual) ano_actual,  " + 
						"sum(v.asignado) asignado, sum(v.vigente) vigente     " + 
						"from ( select ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, fuente, grupo, subgrupo, renglon, geografico, " +
						"sum(asignado) asignado, sum(vigente) vigente from dashboard.mv_vigente " +
						"where ejercicio = ? " +
						"group by ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, fuente, grupo, subgrupo, renglon, geografico " +
						") v left outer join ( select ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, fuente, grupo, subgrupo, renglon, geografico, " +
						"sum(ano_actual) ano_actual from dashboard.mv_gasto " +
						"where ejercicio= ? " +
						"group by  ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, fuente, grupo, subgrupo, renglon, geografico " +
						") g  " + 
						"on(  g.ejercicio = v.ejercicio " +
						"and g.mes = v.mes   " + 
						"and g.entidad = v.entidad  " + 
						"and g.unidad_ejecutora = v.unidad_ejecutora  " + 
						"and g.programa = v.programa  " + 
						"and g.subprograma = v.subprograma  " + 
						"and g.proyecto = v.proyecto  " + 
						"and g.actividad = v.actividad  " + 
						"and g.obra = v.obra  " + 
						"and g.mes = v.mes  " + 
						"and g.fuente = v.fuente  " + 
						"and g.grupo = v.grupo  " + 
						"and g.subgrupo = v.subgrupo  " + 
						"and g.renglon = v.renglon  " + 
						"and g.geografico = v.geografico   " + 
						"and g.ejercicio = ?) " +
						"group by v.ejercicio, v.mes, v.entidad, " + 
						"v.unidad_ejecutora, v.programa, v.subprograma, " + 
						"v.proyecto, v.actividad, v.obra, v.fuente, " + 
						"v.grupo, v.subgrupo, v.renglon,  v.geografico " );
				pstm.setInt(1, ejercicio);
				pstm.setInt(2, ejercicio);
				pstm.setInt(3, ejercicio);
				pstm.executeUpdate();
				pstm.close(); 
				
				CLogger.writeConsole("Insertando valores a MV_EJECUCION_PRESUPUESTARIA_MENSUALIZADA");
				//Actualiza la vista de mv_ejecucion_presupuestaria
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard.mv_ejecucion_presupuestaria_mensualizada "+
						"select ep.ejercicio, ep.entidad, ep.unidad_ejecutora, ep.programa, ep.subprograma, ep.proyecto, " + 
						"ep.actividad, ep.obra, ep.fuente, ep.renglon, " + 
						"sum(case when ep.mes=1 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=2 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=3 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=4 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=5 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=6 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=7 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=8 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=9 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=10 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=11 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=12 then ep.ano_actual else 0 end) " + 
						"from dashboard.mv_ejecucion_presupuestaria ep " +
						"where ep.ejercicio = ? "+
						"group by ep.ejercicio, ep.entidad, ep.unidad_ejecutora, ep.programa, ep.subprograma, ep.proyecto, ep.actividad, ep.obra, ep.fuente, ep.renglon" );
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				pstm.close(); 
				
				boolean bconn =  CMemSQL.connect();
				CLogger.writeConsole("Cargando datos a cache de MV_EJECUCION_PRESUPUESTARIA");
				if(bconn){
					CMemSQL.getConnection().setAutoCommit(false);
					ret = true;
					int rows = 0;
					int rows_total=0;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ejecucion_presupuestaria(ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, " + 
							"proyecto, actividad, obra, renglon, renglon_nombre, subgrupo, subgrupo_nombre, grupo," + 
							"grupo_nombre, fuente, ano_1, ano_2, ano_3, ano_4, ano_5, ano_actual, solicitado, aprobado, anticipo, asignado, vigente) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?) ");
					for(int i=1; i<13; i++){
						pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ejecucion_presupuestaria where mes = ? and ejercicio = ?");
						pstm.setInt(1, i);
						pstm.setInt(2, ejercicio);
						pstm.setFetchSize(10000);
						ResultSet rs = pstm.executeQuery();
						while(rs!=null && rs.next()){
							if(first){
								PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ejecucion_presupuestaria where ejercicio =  ? ");
								pstm2.setInt(1, ejercicio);
								if (pstm2.executeUpdate()>0)
									CLogger.writeConsole("Registros eliminados");
								else
									CLogger.writeConsole("Sin registros para eliminar");	
								pstm2.close();
								first=false;
							}
							pstm1.setInt(1, rs.getInt("ejercicio"));
							pstm1.setInt(2, rs.getInt("mes"));
							pstm1.setInt(3, rs.getInt("entidad"));
							pstm1.setInt(4, rs.getInt("unidad_ejecutora"));
							pstm1.setInt(5, rs.getInt("programa"));
							pstm1.setInt(6, rs.getInt("subprograma"));
							pstm1.setInt(7, rs.getInt("proyecto"));
							pstm1.setInt(8, rs.getInt("actividad"));
							pstm1.setInt(9, rs.getInt("obra"));
							pstm1.setInt(10, rs.getInt("renglon"));
							pstm1.setString(11, rs.getString("renglon_nombre"));
							pstm1.setInt(12, rs.getInt("subgrupo"));
							pstm1.setString(13, rs.getString("subgrupo_nombre"));
							pstm1.setInt(14, rs.getInt("grupo"));
							pstm1.setString(15, rs.getString("grupo_nombre"));
							pstm1.setInt(16, rs.getInt("fuente"));
							pstm1.setDouble(17, rs.getDouble("ano_1"));
							pstm1.setDouble(18, rs.getDouble("ano_2"));
							pstm1.setDouble(19, rs.getDouble("ano_3"));
							pstm1.setDouble(20, rs.getDouble("ano_4"));
							pstm1.setDouble(21, rs.getDouble("ano_5"));
							pstm1.setDouble(22, rs.getDouble("ano_actual"));
							Double solicitado_cuota=rs.getDouble("solicitado_cuota");
							if(!rs.wasNull())
								pstm1.setDouble(23, solicitado_cuota);
							else
								pstm1.setObject(23, null);
							Double aprobado_cuota=rs.getDouble("aprobado_cuota");
							if(!rs.wasNull())
								pstm1.setDouble(24, aprobado_cuota);
							else
								pstm1.setObject(24, null);
							Double anticipo_cuota=rs.getDouble("anticipo_cuota");
							if(!rs.wasNull())
								pstm1.setDouble(25, anticipo_cuota);
							else
								pstm1.setObject(25, null);
							pstm1.setDouble(26, rs.getDouble("asignado"));
							pstm1.setDouble(27, rs.getDouble("vigente"));
							pstm1.addBatch();
							rows++;
							if((rows % 10000) == 0){
								pstm1.executeBatch();
								CMemSQL.getConnection().commit();
							}
						}
						CLogger.writeConsole("Records escritos: "+rows+" - mes: "+i);
						pstm1.executeBatch();
						rows_total += rows;
						rows=0;
						rs.close();
						pstm.close();
						CMemSQL.getConnection().commit();
					}
					
					CLogger.writeConsole("Records escritos Totales: "+rows_total);
					pstm1.close();
					
					CLogger.writeConsole("Cargando datos a cache de MV_EJECUCION_PRESUPUESTARIA_GEOGRAFICO");
					ret = true;
					rows = 0;
					first=true;
					pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ejecucion_presupuestaria_geografico(ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, " + 
							"proyecto, actividad, obra, renglon, subgrupo, grupo," + 
							"fuente, geografico, ano_actual, asignado, vigente) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?) ");
					for(int i=1; i<13; i++){
						pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ejecucion_presupuestaria_geografico where mes = ? and ejercicio = ? ");
						pstm.setInt(1, i);
						pstm.setInt(2, ejercicio);
						pstm.setFetchSize(10000);
						ResultSet rs = pstm.executeQuery();
						while(rs!=null && rs.next()){
							if(first){
								PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ejecucion_presupuestaria_geografico where ejercicio = ? ");
								pstm2.setInt(1, ejercicio);
								if (pstm2.executeUpdate()>0)
									CLogger.writeConsole("Registros eliminados");
								else
									CLogger.writeConsole("Sin registros para eliminar");	
								pstm2.close();
								first=false;
							}
							pstm1.setInt(1, rs.getInt("ejercicio"));
							pstm1.setInt(2, rs.getInt("mes"));
							pstm1.setInt(3, rs.getInt("entidad"));
							pstm1.setInt(4, rs.getInt("unidad_ejecutora"));
							pstm1.setInt(5, rs.getInt("programa"));
							pstm1.setInt(6, rs.getInt("subprograma"));
							pstm1.setInt(7, rs.getInt("proyecto"));
							pstm1.setInt(8, rs.getInt("actividad"));
							pstm1.setInt(9, rs.getInt("obra"));
							pstm1.setInt(10, rs.getInt("renglon"));
							pstm1.setInt(11, rs.getInt("subgrupo"));
							pstm1.setInt(12, rs.getInt("grupo"));
							pstm1.setInt(13, rs.getInt("fuente"));
							pstm1.setInt(14, rs.getInt("geografico"));
							pstm1.setDouble(15, rs.getDouble("ano_actual"));
							pstm1.setDouble(16, rs.getDouble("asignado"));
							pstm1.setDouble(17, rs.getDouble("vigente"));
							pstm1.addBatch();
							rows++;
							if((rows % 10000) == 0){
								pstm1.executeBatch();
								CMemSQL.getConnection().commit();
							}
						}
						CLogger.writeConsole("Records escritos: "+rows+" - mes: "+i);
						pstm1.executeBatch();
						rows_total += rows;
						rows=0;
						rs.close();
						pstm.close();
						CMemSQL.getConnection().commit();
					}
					
					CLogger.writeConsole("Records escritos Totales: "+rows_total);
					pstm1.close();
					
					CLogger.writeConsole("Cargando datos a cache de MV_ESTRUCTURA");
					ret = true;
					rows = 0;
					first=true;
					pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_estructura(ejercicio, entidad, entidad_nombre, sigla, unidad_ejecutora, unidad_ejecutora_nombre, "
							+ "programa, programa_nombre, subprograma, subprograma_nombre, proyecto, proyecto_nombre, "
							+ "actividad, obra, actividad_obra_nombre) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?) ");
					pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_estructura where ejercicio = ? ");
					pstm.setInt(1, ejercicio);
					pstm.setFetchSize(10000);
					ResultSet rs = pstm.executeQuery();
					while(rs!=null && rs.next()){
						if(first){
							PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_estructura where ejercicio = ? ");
							pstm2.setInt(1, ejercicio);
							if (pstm2.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm2.close();
							first=false;
						}
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("entidad"));
						pstm1.setString(3, rs.getString("entidad_nombre"));
						pstm1.setString(4, rs.getString("sigla"));
						pstm1.setInt(5, rs.getInt("unidad_ejecutora"));
						pstm1.setString(6, rs.getString("unidad_ejecutora_nombre"));
						pstm1.setInt(7, rs.getInt("programa"));
						pstm1.setString(8, rs.getString("programa_nombre"));
						pstm1.setInt(9, rs.getInt("subprograma"));
						pstm1.setString(10, rs.getString("subprograma_nombre"));
						pstm1.setInt(11, rs.getInt("proyecto"));
						pstm1.setString(12, rs.getString("proyecto_nombre"));
						pstm1.setInt(13, rs.getInt("actividad"));
						pstm1.setInt(14, rs.getInt("obra"));
						pstm1.setString(15, rs.getString("actividad_obra_nombre"));
						pstm1.addBatch();
						rows++;
						if((rows % 10000) == 0){
							pstm1.executeBatch();
							CMemSQL.getConnection().commit();
						}
					}
					pstm1.executeBatch();
					rs.close();
					pstm.close();
					CMemSQL.getConnection().commit();
					
					
					CLogger.writeConsole("Cargando datos a cache de MV_EJECUCION_PRESUPUESTARIA_MENSUALIZADA");
					ret = true;
					rows = 0;
					first=true;
					pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ejecucion_presupuestaria_mensualizada(ejercicio, entidad, unidad_ejecutora, "
							+ "programa, subprograma, proyecto,actividad, obra, fuente, renglon, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12) "
							+ "values (?,?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,?) ");
					pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ejecucion_presupuestaria_mensualizada where ejercicio = ? ");
					pstm.setInt(1, ejercicio);
					pstm.setFetchSize(10000);
					rs = pstm.executeQuery();
					while(rs!=null && rs.next()){
						if(first){
							PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ejecucion_presupuestaria_mensualizada where ejercicio = ? ");
							pstm2.setInt(1, ejercicio);
							if (pstm2.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm2.close();
							first=false;
						}
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("entidad"));
						pstm1.setInt(3, rs.getInt("unidad_ejecutora"));
						pstm1.setInt(4, rs.getInt("programa"));
						pstm1.setInt(5, rs.getInt("subprograma"));
						pstm1.setInt(6, rs.getInt("proyecto"));
						pstm1.setInt(7, rs.getInt("actividad"));
						pstm1.setInt(8, rs.getInt("obra"));
						pstm1.setInt(9, rs.getInt("fuente"));
						pstm1.setInt(10, rs.getInt("renglon"));
						pstm1.setDouble(11, rs.getDouble("m1"));
						pstm1.setDouble(12, rs.getDouble("m2"));
						pstm1.setDouble(13, rs.getDouble("m3"));
						pstm1.setDouble(14, rs.getDouble("m4"));
						pstm1.setDouble(15, rs.getDouble("m5"));
						pstm1.setDouble(16, rs.getDouble("m6"));
						pstm1.setDouble(17, rs.getDouble("m7"));
						pstm1.setDouble(18, rs.getDouble("m8"));
						pstm1.setDouble(19, rs.getDouble("m9"));
						pstm1.setDouble(20, rs.getDouble("m10"));
						pstm1.setDouble(21, rs.getDouble("m11"));
						pstm1.setDouble(22, rs.getDouble("m12"));
						pstm1.addBatch();
						rows++;
						if((rows % 10000) == 0){
							pstm1.executeBatch();
							CMemSQL.getConnection().commit();
						}
					}
					pstm1.executeBatch();
					rs.close();
					pstm.close();
					CMemSQL.getConnection().commit();
					
					CLogger.writeConsole("Records escritos Totales: "+rows);
					pstm1.close(); 
				}
				
			}					
				
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CEjecucionPresupuestaria.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
public static boolean loadEjecucionPresupuestariaHistoria(Connection conn, Integer ejercicio_inicio, Integer ejercicio_fin){
		
		boolean ret = false;
		try{
			if( !conn.isClosed() && CMemSQL.connect()){
				ret = true;

				CLogger.writeConsole("CEjecucionPresupuestaria Entidades Historia (Ejercicios "+ejercicio_inicio+" a "+ejercicio_fin+"):");
				CLogger.writeConsole("Eliminando data actual:");
				PreparedStatement pstm;
				List<String> tablas = Arrays.asList("mv_estructura", "mv_cuota", "mv_gasto", "mv_anticipo","mv_vigente", "mv_ejecucion_presupuestaria","mv_ejecucion_presupuestaria_geografico", "mv_ejecucion_presupuestaria_mensualizada");
				
				for(String tabla:tablas){
					CLogger.writeConsole("Copiando historia - "+tabla);
					pstm = conn.prepareStatement("DROP TABLE IF EXISTS dashboard_historia."+tabla+"_temp PURGE");
					pstm.executeUpdate();
					pstm.close();
					pstm = conn.prepareStatement("CREATE TABLE dashboard_historia."+tabla+"_temp AS SELECT * FROM dashboard_historia."+tabla+" WHERE ejercicio not between ? and ?");
					pstm.setInt(1, ejercicio_inicio);
					pstm.setInt(2, ejercicio_fin);
					pstm.executeUpdate();
					pstm.close();
					pstm = conn.prepareStatement("TRUNCATE TABLE dashboard_historia."+tabla);
					pstm.setInt(1, ejercicio_inicio);
					pstm.setInt(2, ejercicio_fin);
					pstm.executeUpdate();
					pstm.close();
					pstm = conn.prepareStatement("INSERT INTO dashboard_historia."+tabla+" SELECT * FROM dashboard_historia."+tabla+"_temp");
					pstm.setInt(1, ejercicio_inicio);
					pstm.setInt(2, ejercicio_fin);
					pstm.executeUpdate();
					pstm.close();
					pstm = conn.prepareStatement("DROP TABLE IF EXISTS dashboard_historia."+tabla+"_temp PURGE");
					pstm.executeUpdate();
					pstm.close();
				}
				
				
				CLogger.writeConsole("Insertando valores a MV_ESTRUCTURA");
				pstm = conn.prepareStatement("insert into table dashboard_historia.mv_estructura "+
						"select e.ejercicio, e.entidad, e.nombre entidad_nombre, es.sigla, ue.unidad_ejecutora, ue.nombre unidad_ejecutora_nombre,  " + 
						"p.programa, p.nom_estructura programa_nombre,  " + 
						"sp.subprograma, sp.nom_estructura subprograma_nombre,  " + 
						"pr.proyecto, pr.nom_estructura proyecto_nombre,  " + 
						"ao.actividad, ao.obra, ao.nom_estructura actividad_obra_nombre  " + 
						"from sicoinprod.cg_entidades e, sicoinprod.cg_entidades ue " + 
						"left outer join sicoinprod.cp_estructuras p  " + 
						"on( " + 
						"p.ejercicio = ue.ejercicio  " + 
						"and p.entidad = ue.entidad  " + 
						"and p.unidad_ejecutora = ue.unidad_ejecutora  " + 
						"and p.nivel_estructura = 2 " + 
						") left outer join sicoinprod.cp_estructuras sp  " + 
						"on( " + 
						"sp.ejercicio = ue.ejercicio  " + 
						"and sp.entidad = ue.entidad  " + 
						"and sp.unidad_ejecutora = ue.unidad_ejecutora  " + 
						"and sp.programa = p.programa  " + 
						"and sp.nivel_estructura = 3  " + 
						") left outer join sicoinprod.cp_estructuras pr " + 
						"on( " + 
						"pr.ejercicio = ue.ejercicio  " + 
						"and pr.entidad = ue.entidad   " + 
						"and pr.unidad_ejecutora = ue.unidad_ejecutora  " + 
						"and pr.programa = p.programa  " + 
						"and pr.subprograma = sp.subprograma  " + 
						"and pr.nivel_estructura = 4  " + 
						") left outer join sicoinprod.cp_estructuras ao " + 
						"on( " + 
						"ao.ejercicio = ue.ejercicio  " + 
						"and ao.entidad = ue.entidad  " + 
						"and ao.unidad_ejecutora = ue.unidad_ejecutora  " + 
						"and ao.programa = p.programa  " + 
						"and ao.subprograma = sp.subprograma  " + 
						"and ao.proyecto = pr.proyecto  " + 
						"and ao.nivel_estructura = 5 " + 
						"), dashboard.mv_entidad mve, dashboard.entidad_sigla es  " + 
						"where e.ejercicio between ? and ? " + 
						"and ue.ejercicio = e.ejercicio  " + 
						"and e.restrictiva = 'N'  " + 
						"and ue.restrictiva = 'N'  " + 
						"and e.unidad_ejecutora = 0  " + 
						"and ue.entidad = e.entidad  " + 
						"and ((e.entidad between 11130000 and 11130020) OR e.entidad = 11140021)   " + 
						"and mve.entidad = e.entidad  " + 
						"and mve.ejercicio = e.ejercicio  " + 
						"and ((ue.unidad_ejecutora = 0 and mve.unidades_ejecutoras=1) or (ue.unidad_ejecutora>0 and mve.unidades_ejecutoras>1))  " + 
						"and es.entidad = e.entidad");
				pstm.setInt(1, ejercicio_inicio);
				pstm.setInt(2, ejercicio_fin);
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_GASTO");
				///Actualiza la vista de gasto
				for(int i=ejercicio_inicio; i<=ejercicio_fin ; i++){
					pstm = conn.prepareStatement("insert into table dashboard_historia.mv_gasto "
							+"select "+i+" ejercicio,month(gh.fec_aprobado) mes, gd.entidad, gd.unidad_ejecutora, gd.programa, gd.subprograma, gd.proyecto, gd.actividad, gd.obra, gd.renglon, r.nombre renglon_nombre, gd.fuente,     " + 
							"						 						 	gd.renglon - (gd.renglon%100) grupo, gg.nombre grupo_nombre, gd.renglon - (gd.renglon%10) subgrupo, sg.nombre subgrupo_nombre, gd.geografico,      " + 
							"						 						 	sum( case when gh.ejercicio = (? - 5) then gd.monto_renglon else 0 end) ano_1,      " + 
							"						 						 	sum( case when gh.ejercicio = (? - 4) then gd.monto_renglon else 0 end) ano_2,      " + 
							"						 						 	sum( case when gh.ejercicio = (? - 3) then gd.monto_renglon else 0 end) ano_3,      " + 
							"						 						 	sum( case when gh.ejercicio = (? - 2) then gd.monto_renglon else 0 end) ano_4,      " + 
							"						 						 	sum( case when gh.ejercicio = (? - 1) then gd.monto_renglon else 0 end) ano_5,      " + 
							"						 						 	sum( case when gh.ejercicio = ? then gd.monto_renglon else 0 end) ano_actual      " + 
							"						 						 				from sicoinprod.eg_gastos_hoja gh, sicoinprod.eg_gastos_detalle gd,      " + 
							"						 										sicoinprod.cp_grupos_gasto gg, sicoinprod.cp_objetos_gasto sg, sicoinprod.cp_objetos_gasto r  		 " + 
							"						 						 				where gh.ejercicio = gd.ejercicio         " + 
							"						 						 				and gh.entidad = gd.entidad       " + 
							"						 						 				and gh.unidad_ejecutora = gd.unidad_ejecutora       " + 
							"						 						 				and gh.no_cur = gd.no_cur       " + 
							"						 						 				and gh.clase_registro IN ('DEV', 'CYD', 'RDP', 'REG')       " + 
							"						 						 				and gh.estado = 'APROBADO'       " + 
							"						 						 				and gh.ejercicio > ( ? - 6 )       " + 
							"						  										and gg.ejercicio =  ? " + 
							"		 				  										and gg.grupo_gasto = (gd.renglon-(gd.renglon%100))   " + 
							"		 				  										and sg.ejercicio = ?    " + 
							"		 				  										and sg.renglon = (gd.renglon - (gd.renglon%10))        " + 
							"		 				  										and r.ejercicio = ?   " + 
							"		 				  										and r.renglon = gd.renglon   " + 
							"						 						 				group by month(gh.fec_aprobado), gd.entidad, gd.unidad_ejecutora, gd.programa, gd.subprograma,       " + 
							"						 						 				gd.proyecto, gd.actividad, gd.obra, gg.nombre, sg.nombre, r.nombre, gd.renglon, gd.fuente, gd.geografico  ");
					pstm.setInt(1, i);
					pstm.setInt(2, i);
					pstm.setInt(3, i);
					pstm.setInt(4, i);
					pstm.setInt(5, i);
					pstm.setInt(6, i);
					pstm.setInt(7, i);
					pstm.setInt(8, i);
					pstm.setInt(9, i);
					pstm.setInt(10, i);
	 				pstm.executeUpdate();
					pstm.close();
				}
				
				CLogger.writeConsole("Insertando valores a MV_CUOTA");
				///Actualiza la vista de cuota
				pstm = conn.prepareStatement("INSERT INTO table dashboard_historia.mv_cuota "+
						"SELECT d.ejercicio, t.mes,  " + 
						"                   d.entidad,  " + 
						"                   d.unidad_ejecutora,  " + 
						"                   d.fuente,   " + 
						"                   sum(case  " + 
						"                   when t.mes = 1 and d.cuatrimestre = 1 then d.cuota_mes1_sol  " + 
						"                   when t.mes = 2 and d.cuatrimestre = 1 then d.cuota_mes2_sol " + 
						"                   when t.mes = 3 and d.cuatrimestre = 1 then d.cuota_mes3_sol " + 
						"                   when t.mes = 4 and d.cuatrimestre = 1 then d.cuota_mes4_sol " + 
						"                   when t.mes = 5 and d.cuatrimestre = 2 then d.cuota_mes1_sol  " + 
						"                   when t.mes = 6 and d.cuatrimestre = 2 then d.cuota_mes2_sol " + 
						"                   when t.mes = 7 and d.cuatrimestre = 2 then d.cuota_mes3_sol " + 
						"                   when t.mes = 8 and d.cuatrimestre = 2 then d.cuota_mes4_sol " + 
						"                   when t.mes = 9 and d.cuatrimestre = 3 then d.cuota_mes1_sol  " + 
						"                   when t.mes = 10 and d.cuatrimestre = 3 then d.cuota_mes2_sol " + 
						"                   when t.mes = 11 and d.cuatrimestre = 3 then d.cuota_mes3_sol " + 
						"                   when t.mes = 12 and d.cuatrimestre = 3 then d.cuota_mes4_sol " + 
						"                   end ) solicitado, " + 
						"                   sum(case  " + 
						"                   when t.mes = 1 and d.cuatrimestre = 1 then d.cuota_mes1_apr " + 
						"                   when t.mes = 2 and d.cuatrimestre = 1 then d.cuota_mes2_apr " + 
						"                   when t.mes = 3 and d.cuatrimestre = 1 then d.cuota_mes3_apr " + 
						"                   when t.mes = 4 and d.cuatrimestre = 1 then d.cuota_mes4_apr " + 
						"                   when t.mes = 5 and d.cuatrimestre = 2 then d.cuota_mes1_apr " + 
						"                   when t.mes = 6 and d.cuatrimestre = 2 then d.cuota_mes2_apr " + 
						"                   when t.mes = 7 and d.cuatrimestre = 2 then d.cuota_mes3_apr " + 
						"                   when t.mes = 8 and d.cuatrimestre = 2 then d.cuota_mes4_apr " + 
						"                   when t.mes = 9 and d.cuatrimestre = 3 then d.cuota_mes1_apr " + 
						"                   when t.mes = 10 and d.cuatrimestre = 3 then d.cuota_mes2_apr " + 
						"                   when t.mes = 11 and d.cuatrimestre = 3 then d.cuota_mes3_apr " + 
						"                   when t.mes = 12 and d.cuatrimestre = 3 then d.cuota_mes4_apr " + 
						"                   end ) aprobado " + 
						"                   FROM sicoinprod.EG_FINANCIERO_DETALLE_4 D,  " + 
						"                   sicoinprod.eg_financiero_hoja_4 H1, dashboard.tiempo t  " + 
						"                   WHERE  h1.ejercicio between ? and ?   " + 
						"                   and h1.ejercicio = d.ejercicio " + 
						"                    and t.ejercicio = h1.ejercicio " + 
						"                    and t.dia = 1 " + 
						"                    AND h1.entidad = d.entidad  " + 
						"                    AND h1.unidad_ejecutora = d.unidad_ejecutora  " + 
						"                    AND h1.unidad_desconcentrada = d.unidad_desconcentrada  " + 
						"                    AND h1.no_cur = d.no_cur  " + 
						"                    AND H1.CLASE_REGISTRO IN ('RPG', 'PRG', 'RPGI')  " + 
						"                    AND H1.estado = 'APROBADO'  " + 
						"                    GROUP BY d.ejercicio,  " + 
						"                    t.mes, " + 
						"                    d.entidad,  " + 
						"                    d.unidad_ejecutora,  " + 
						"                    d.fuente "
						);	
				pstm.setInt(1, ejercicio_inicio);
				pstm.setInt(2, ejercicio_fin);
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_ANTICIPO");
				///Actualiza la vista de cuota
				pstm = conn.prepareStatement("INSERT INTO table dashboard_historia.mv_anticipo "+
						"SELECT d.ejercicio, " + 
						" 		t.mes,   " + 
						"        d.entidad,   " + 
						"        d.unidad_ejecutora,   " + 
						"        d.fuente,   " + 
						"        sum(d.cuota_fondo_rotativo_apr + d.cuota_fideicomisos_apr + d.CUOTA_CONVENIOS_APR + d.cuota_contratos_apr + d.cuota_otros_apr + d.cuota_paa_apr) anticipo   " + 
						"	    FROM  sicoinprod.eg_anticipo_hoja h,   " + 
						"	          sicoinprod.eg_anticipo_detalle d, " + 
						"	          dashboard.tiempo t   " + 
						"	    WHERE h.ejercicio = d.ejercicio   " + 
						"	    AND h.ejercicio between ? and ? " + 
						"	    and t.ejercicio = h.ejercicio " + 
						"	    and t.dia = 1 " + 
						"	    and d.cuatrimestre = ceil(t.mes/4) " + 
						"	    and d.mes = t.mes-((d.cuatrimestre-1)*4) " + 
						"	    AND   h.entidad = d.entidad   " + 
						"	    AND   h.unidad_ejecutora = d.unidad_ejecutora   " + 
						"	    AND   h.no_cur = d.no_cur   " + 
						"	    AND   h.estado = 'APROBADO'   " + 
						"	    AND   h.clase_registro IN ('PRG','RPG')   " + 
						"	    group by d.ejercicio, t.mes, d.entidad, d.unidad_ejecutora, d.fuente"
						);	
				pstm.setInt(1, ejercicio_inicio);
				pstm.setInt(2, ejercicio_fin);
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_VIGENTE");
				//Actualiza la vista de mv_vigente
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard_historia.mv_vigente " + 
						"select asignado.ejercicio, asignado.mes, asignado.entidad, asignado.unidad_ejecutora, asignado.programa, asignado.subprograma, asignado.proyecto, asignado.actividad, asignado.obra,    " + 
						"						asignado.fuente, asignado.grupo, gg.nombre grupo_nombre, asignado.subgrupo,  sg.nombre subgrupo_nombre, " + 
						"						asignado.renglon, r.nombre renglon_nombre, asignado.geografico,   " + 
						"						sum(asignado.asignado) asignado, sum(asignado.asignado)+nvl(sum(vigente.modificaciones),0) vigente   " + 
						"						from (   " + 
						"							select t.ejercicio,      " + 
						"													t.mes,      " + 
						"													a.entidad,      " + 
						"													a.unidad_ejecutora,      " + 
						"													a.programa,      " + 
						"													a.subprograma,      " + 
						"													a.proyecto,      " + 
						"													a.actividad,      " + 
						"													a.obra,      " + 
						"													a.fuente,      " + 
						"													(a.renglon-a.renglon%100) grupo,      " + 
						"													(a.renglon-a.renglon%10) subgrupo,      " + 
						"													a.renglon,      " + 
						"													a.geografico,      " + 
						"													sum(a.asignado) asignado      " + 
						"													from dashboard.tiempo t      " + 
						"													left outer join      " + 
						"													(select a.ejercicio, a.entidad, a.unidad_ejecutora, a.programa, a.subprograma, a.proyecto, a.actividad, a.obra, a.fuente,      " + 
						"														a.renglon, a.geografico, a.asignado      " + 
						"														from sicoinprod.eg_f6_partidas a) a on (a.ejercicio = t.ejercicio and t.dia=1 and t.ejercicio between ? and ?)    " + 
						"														left outer join (   " + 
						"															select ejercicio, entidad, count(*) ues    " + 
						"															from sicoinprod.cg_entidades   " + 
						"															group by ejercicio, entidad ) ues on ( ues.ejercicio = t.ejercicio and ues.entidad = a.entidad)   " + 
						"													where ((a.unidad_ejecutora>0 and ues.ues>0) OR (a.unidad_ejecutora=0 and ues.ues=1))   " + 
						"													group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14 " + 
						"								 " + 
						"						) asignado left outer join   " + 
						"						(   " + 
						"						select t.ejercicio, t.mes, m.entidad, m.unidad_ejecutora, m.programa, m.subprograma, m.proyecto, m.actividad, m.obra, m.fuente, m.renglon, m.geografico,  " + 
						"						sum(case when m.mes<=t.mes then m.modificaciones else 0 end) modificaciones " + 
						"						from dashboard.tiempo t left outer join ( " + 
						"							select mh.ejercicio, month(mh.fec_aprobado) mes, md.entidad, md.unidad_ejecutora, md.programa, md.subprograma, md.proyecto, md.actividad, md.obra, md.fuente, md.renglon,   " + 
						"							md.geografico,   " + 
						"							sum(md.monto_aprobado) modificaciones   " + 
						"								from sicoinprod.eg_modificaciones_hoja mh left outer join    " + 
						"								(select ejercicio, entidad, count(*) ues   " + 
						"								from sicoinprod.cg_entidades   " + 
						"								group by ejercicio, entidad) num_ues on (mh.entidad=num_ues.entidad and mh.ejercicio = num_ues.ejercicio), sicoinprod.eg_modificaciones_detalle md   " + 
						"								where md.ejercicio = mh.ejercicio   " + 
						"								and num_ues.ejercicio = mh.ejercicio   " + 
						"								and num_ues.entidad = mh.entidad   " + 
						"								and md.clase_registro = mh.clase_registro   " + 
						"								and md.no_cur = mh.no_cur   " + 
						"								and md.unidad_ejecutora = mh.unidad_ejecutora   " + 
						"								and md.entidad = mh.entidad   " + 
						"								and mh.APROBADO = 'S'   " + 
						"								and ((mh.unidad_ejecutora>0 and num_ues.ues>0) OR (mh.unidad_ejecutora=0 and num_ues.ues=1))   " + 
						"								and md.ejercicio = mh.ejercicio " + 
						"								group by mh.ejercicio, month(mh.fec_aprobado), md.mes_modificacion, md.entidad,  md.unidad_ejecutora,  md.programa,  md.subprograma,  md.proyecto,  md.actividad,  md.obra,     " + 
						"								md.renglon, md.fuente, md.geografico  " + 
						"							) m on (t.ejercicio = m.ejercicio and t.dia=1 and t.ejercicio between ? and ?)  " + 
						"							group by t.ejercicio, t.mes, m.entidad, m.unidad_ejecutora, m.programa, m.subprograma, m.proyecto, m.actividad, m.obra, m.fuente, m.renglon, m.geografico  " + 
						"						) vigente   " + 
						"						on (asignado.ejercicio = vigente.ejercicio   " + 
						"						and asignado.mes = vigente.mes    " + 
						"						and asignado.entidad = vigente.entidad   " + 
						"						and asignado.unidad_ejecutora = vigente.unidad_ejecutora    " + 
						"						and asignado.programa = vigente.programa   " + 
						"						and asignado.subprograma = vigente.subprograma   " + 
						"						and asignado.proyecto = vigente.proyecto   " + 
						"						and asignado.actividad = vigente.actividad   " + 
						"						and asignado.obra = vigente.obra   " + 
						"						and asignado.fuente = vigente.fuente " + 
						"						and asignado.renglon = vigente.renglon   " + 
						"						and asignado.geografico = vigente.geografico), sicoinprod.cp_grupos_gasto gg, sicoinprod.cp_objetos_gasto sg, sicoinprod.cp_objetos_gasto r " + 
						"						where gg.grupo_gasto = asignado.grupo  " + 
						"						and gg.ejercicio = asignado.ejercicio    " + 
						"						and sg.renglon = asignado.subgrupo   " + 
						"						and sg.ejercicio = asignado.ejercicio " + 
						"						and r.ejercicio = asignado.ejercicio   " + 
						"						and r.renglon = asignado.renglon " + 
						"						group by asignado.ejercicio, asignado.mes, asignado.entidad, asignado.unidad_ejecutora, asignado.programa, asignado.subprograma, asignado.proyecto, asignado.actividad, asignado.obra,    " + 
						"						asignado.fuente, asignado.grupo, gg.nombre, asignado.subgrupo, sg.nombre, asignado.renglon, r.nombre, asignado.geografico  ");
				pstm.setInt(1, ejercicio_inicio);
				pstm.setInt(2, ejercicio_fin);
				pstm.setInt(3, ejercicio_inicio);
				pstm.setInt(4, ejercicio_fin);
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_EJECUCION_PRESUPUESTARIA");
				//Actualiza la vista de mv_ejecucion_presupuestaria
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard_historia.mv_ejecucion_presupuestaria  "+
						"select    " + 
						"												t1.ejercicio,    " + 
						"												t1.entidad,    " + 
						"												t1.unidad_ejecutora,    " + 
						"												t1.programa,    " + 
						"												t1.subprograma,    " + 
						"												t1.proyecto,    " + 
						"												t1.actividad,    " + 
						"												t1.obra,    " + 
						"												t1.mes, t1.fuente, t1.grupo, t1.grupo_nombre, t1.subgrupo, t1.subgrupo_nombre, t1.renglon, t1.renglon_nombre,     " + 
						"																		 t1.ano_1, t1.ano_2, t1.ano_3, t1.ano_4, t1.ano_5, t1.ano_actual, t1.asignado, t1.vigente, a.anticipo anticipo_cuota, c.solicitado solicitado_cuota,      " + 
						"																		 c.aprobado aprobado_cuota       " + 
						"																		 from (      " + 
						"																		 	select " +
						"																			nvl(v.ejercicio,g.ejercicio) ejercicio, "+	
						"																			nvl(v.mes,g.mes) mes,       " + 
						"																		 	nvl(v.entidad,g.entidad) entidad,      " + 
						"																		 	nvl(v.unidad_ejecutora,g.unidad_ejecutora) unidad_ejecutora,     " + 
						"																		 	nvl(v.programa,g.programa) programa,      " + 
						"																		 	nvl(v.subprograma,g.subprograma) subprograma,      " + 
						"																		 	nvl(v.proyecto, g.proyecto) proyecto,     " + 
						"																		 	nvl(v.actividad,g.actividad) actividad,      " + 
						"																		 	nvl(v.obra,g.obra) obra,      " + 
						"																		 	nvl(v.fuente,g.fuente) fuente,      " + 
						"																		 	nvl(v.grupo,g.grupo) grupo,      " + 
						"																		 	nvl(v.grupo_nombre,g.grupo_nombre) grupo_nombre,      " + 
						"																		 	nvl(v.subgrupo, g.subgrupo) subgrupo,     " + 
						"																		 	nvl(v.subgrupo_nombre,g.subgrupo_nombre) subgrupo_nombre,    " + 
						"																		 	nvl(v.renglon,g.renglon) renglon,      " + 
						"																		 	nvl(v.renglon_nombre, g.renglon_nombre) renglon_nombre,      " + 
						"																			g.ano_1 ano_1, g.ano_2 ano_2, g.ano_3 ano_3, g.ano_4 ano_4, g.ano_5 ano_5, g.ano_actual ano_actual,      " + 
						"																		 	v.asignado asignado, v.vigente vigente      " + 
						"																		 	from  (	select ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, grupo, grupo_nombre, " + 
						"								    										   subgrupo, subgrupo_nombre, renglon, renglon_nombre, fuente, sum(asignado) asignado, sum(vigente) vigente " + 
						"																			   from dashboard_historia.mv_vigente " + 
						"																			   where ejercicio between ? and ? " + 
						"																			   group by ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, grupo, grupo_nombre, subgrupo, subgrupo_nombre, renglon, renglon_nombre, fuente " + 
						"																			) v full outer join   (" + 
						"																		 		select g1.ejercicio, g1.mes, g1.entidad, g1.unidad_ejecutora, g1.programa, g1.subprograma, g1.proyecto, g1.actividad, g1.obra, g1.fuente," + 
						"																		 		g1.grupo, g1.grupo_nombre, g1.subgrupo, g1.subgrupo_nombre, g1.renglon, g1.renglon_nombre," + 
						"																		 		sum(g1.ano_1) ano_1, sum(g1.ano_2) ano_2, sum(g1.ano_3) ano_3, sum(g1.ano_4) ano_4, sum(g1.ano_5) ano_5," + 
						"																		 		sum(g1.ano_actual) ano_actual" + 
						"																		 		from dashboard_historia.mv_gasto g1 " +
						"																				where g1.ejercicio between ? and ? " +		
						"																		 		group by g1.ejercicio, g1.mes, g1.entidad, g1.unidad_ejecutora, g1.programa, g1.subprograma, g1.proyecto, g1.actividad, g1.obra, g1.fuente, " + 
						"																		 		g1.grupo, g1.grupo_nombre, g1.subgrupo, g1.subgrupo_nombre, g1.renglon, g1.renglon_nombre" + 
						"																		 	) g "+
						"																		 	on(       " + 
						"																		 		g.entidad = v.entidad      " + 
						"																		 		and g.unidad_ejecutora = v.unidad_ejecutora      " + 
						"																		 		and g.programa = v.programa      " + 
						"																		 		and g.subprograma = v.subprograma      " + 
						"																		 		and g.proyecto = v.proyecto      " + 
						"																		 		and g.actividad = v.actividad      " + 
						"																		 		and g.obra = v.obra      " + 
						"																		 		and g.renglon = v.renglon      " + 
						"																		 		and g.fuente = v.fuente      " + 
						"																		 		and g.mes = v.mes      " + 
						"																		 		and g.ejercicio = v.ejercicio      " + 
						"																		 	  )    " + 
						"																		 ) t1    " + 
						"																		 left outer join dashboard_historia.mv_cuota c   " + 
						"																		 on (   " + 
						" 																			c.ejercicio = t1.ejercicio " +
						"																		 	and c.entidad = t1.entidad      " + 
						"																		 	and c.unidad_ejecutora = t1.unidad_ejecutora      " + 
						"																		 	and c.fuente = t1.fuente      " + 
						"																		 	and c.mes = t1.mes   " + 
						"																		 )   " + 
						"																		 left outer join dashboard_historia.mv_anticipo a  " + 
						"																		 on(  " + 
						"																			a.ejercicio = t1.ejercicio " +
						"																		 	and a.entidad = t1.entidad  " + 
						"																		 	and a.unidad_ejecutora = t1.unidad_ejecutora  " + 
						"																		 	and a.fuente = t1.fuente  " + 
						"																		 	and a.mes = t1.mes  " + 
						"																		 ) " );
				pstm.setInt(1, ejercicio_inicio);
				pstm.setInt(2, ejercicio_fin);
				pstm.setInt(3, ejercicio_inicio);
				pstm.setInt(4, ejercicio_fin);
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_EJECUCION_PRESUPUESTARIA_GEOGRAFICO");
				//Actualiza la vista de mv_ejecucion_presupuestaria
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard_historia.mv_ejecucion_presupuestaria_geografico "+
						"select " + 
						"v.ejercicio, v.mes,  " + 
						"v.entidad,  " + 
						"v.unidad_ejecutora,  " + 
						"v.programa,  " + 
						"v.subprograma,  " + 
						"v.proyecto,  " + 
						"v.actividad,  " + 
						"v.obra,  " + 
						"v.fuente,     " + 
						"v.grupo,  " + 
						"v.subgrupo,  " + 
						"v.renglon,  " + 
						"v.geografico,   " + 
						"sum(g.ano_actual) ano_actual,  " + 
						"sum(v.asignado) asignado, sum(v.vigente) vigente     " + 
						"from ( select ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, fuente, grupo, subgrupo, renglon, geografico, " +
						"sum(asignado) asignado, sum(vigente) vigente from dashboard_historia.mv_vigente " +
						"where ejercicio between ? and ? " +
						"group by ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, fuente, grupo, subgrupo, renglon, geografico " +
						") v left outer join ( select ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, fuente, grupo, subgrupo, renglon, geografico, " +
						"sum(ano_actual) ano_actual from dashboard_historia.mv_gasto " +
						"where ejercicio between ? and ? " +
						"group by  ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, fuente, grupo, subgrupo, renglon, geografico " +
						") g  " + 
						"on(  g.ejercicio = v.ejercicio " +
						"and g.mes = v.mes   " + 
						"and g.entidad = v.entidad  " + 
						"and g.unidad_ejecutora = v.unidad_ejecutora  " + 
						"and g.programa = v.programa  " + 
						"and g.subprograma = v.subprograma  " + 
						"and g.proyecto = v.proyecto  " + 
						"and g.actividad = v.actividad  " + 
						"and g.obra = v.obra  " + 
						"and g.mes = v.mes  " + 
						"and g.fuente = v.fuente  " + 
						"and g.grupo = v.grupo  " + 
						"and g.subgrupo = v.subgrupo  " + 
						"and g.renglon = v.renglon  " + 
						"and g.geografico = v.geografico   " + 
						"and g.ejercicio between ? and ?) " +
						"group by v.ejercicio, v.mes, v.entidad, " + 
						"v.unidad_ejecutora, v.programa, v.subprograma, " + 
						"v.proyecto, v.actividad, v.obra, v.fuente, " + 
						"v.grupo, v.subgrupo, v.renglon,  v.geografico " );
				pstm.setInt(1, ejercicio_inicio);
				pstm.setInt(2, ejercicio_fin);
				pstm.setInt(3, ejercicio_inicio);
				pstm.setInt(4, ejercicio_fin);
				pstm.setInt(5, ejercicio_inicio);
				pstm.setInt(6, ejercicio_fin);
				pstm.executeUpdate();
				pstm.close(); 
				
				CLogger.writeConsole("Insertando valores a MV_EJECUCION_PRESUPUESTARIA_MENSUALIZADA");
				//Actualiza la vista de mv_ejecucion_presupuestaria
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard_historia.mv_ejecucion_presupuestaria_mensualizada "+
						"select ep.ejercicio, ep.entidad, ep.unidad_ejecutora, ep.programa, ep.subprograma, ep.proyecto, " + 
						"ep.actividad, ep.obra, ep.fuente, ep.renglon, " + 
						"sum(case when ep.mes=1 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=2 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=3 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=4 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=5 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=6 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=7 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=8 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=9 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=10 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=11 then ep.ano_actual else 0 end), " + 
						"sum(case when ep.mes=12 then ep.ano_actual else 0 end) " + 
						"from dashboard_historia.mv_ejecucion_presupuestaria ep " +
						"where ep.ejercicio between ? and ? "+
						"group by ep.ejercicio, ep.entidad, ep.unidad_ejecutora, ep.programa, ep.subprograma, ep.proyecto, ep.actividad, ep.obra, ep.fuente, ep.renglon" );
				pstm.setInt(1, ejercicio_inicio);
				pstm.setInt(2, ejercicio_fin);
				pstm.executeUpdate();
				pstm.close(); 
				
				boolean bconn =  CMemSQL.connect();
				CLogger.writeConsole("Cargando datos a cache de MV_EJECUCION_PRESUPUESTARIA");
				if(bconn){
					ret = true;
					int rows = 0;
					int rows_total=0;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ejecucion_presupuestaria(ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, " + 
							"proyecto, actividad, obra, renglon, renglon_nombre, subgrupo, subgrupo_nombre, grupo," + 
							"grupo_nombre, fuente, ano_1, ano_2, ano_3, ano_4, ano_5, ano_actual, solicitado, aprobado, anticipo, asignado, vigente) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?) ");
					for(int i=1; i<13; i++){
						pstm = conn.prepareStatement("SELECT * FROM dashboard_historia.mv_ejecucion_presupuestaria where mes = ? and ejercicio between ? and ?");
						pstm.setInt(1, i);
						pstm.setInt(2, ejercicio_inicio);
						pstm.setInt(3, ejercicio_fin);
						pstm.setFetchSize(10000);
						ResultSet rs = pstm.executeQuery();
						while(rs!=null && rs.next()){
							if(first){
								PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ejecucion_presupuestaria where ejercicio between  ? and ? ");
								pstm2.setInt(1, ejercicio_inicio);
								pstm2.setInt(2, ejercicio_fin);
								if (pstm2.executeUpdate()>0)
									CLogger.writeConsole("Registros eliminados");
								else
									CLogger.writeConsole("Sin registros para eliminar");	
								pstm2.close();
								first=false;
							}
							pstm1.setInt(1, rs.getInt("ejercicio"));
							pstm1.setInt(2, rs.getInt("mes"));
							pstm1.setInt(3, rs.getInt("entidad"));
							pstm1.setInt(4, rs.getInt("unidad_ejecutora"));
							pstm1.setInt(5, rs.getInt("programa"));
							pstm1.setInt(6, rs.getInt("subprograma"));
							pstm1.setInt(7, rs.getInt("proyecto"));
							pstm1.setInt(8, rs.getInt("actividad"));
							pstm1.setInt(9, rs.getInt("obra"));
							pstm1.setInt(10, rs.getInt("renglon"));
							pstm1.setString(11, rs.getString("renglon_nombre"));
							pstm1.setInt(12, rs.getInt("subgrupo"));
							pstm1.setString(13, rs.getString("subgrupo_nombre"));
							pstm1.setInt(14, rs.getInt("grupo"));
							pstm1.setString(15, rs.getString("grupo_nombre"));
							pstm1.setInt(16, rs.getInt("fuente"));
							pstm1.setDouble(17, rs.getDouble("ano_1"));
							pstm1.setDouble(18, rs.getDouble("ano_2"));
							pstm1.setDouble(19, rs.getDouble("ano_3"));
							pstm1.setDouble(20, rs.getDouble("ano_4"));
							pstm1.setDouble(21, rs.getDouble("ano_5"));
							pstm1.setDouble(22, rs.getDouble("ano_actual"));
							Double solicitado_cuota=rs.getDouble("solicitado_cuota");
							if(!rs.wasNull())
								pstm1.setDouble(23, solicitado_cuota);
							else
								pstm1.setObject(23, null);
							Double aprobado_cuota=rs.getDouble("aprobado_cuota");
							if(!rs.wasNull())
								pstm1.setDouble(24, aprobado_cuota);
							else
								pstm1.setObject(24, null);
							Double anticipo_cuota=rs.getDouble("anticipo_cuota");
							if(!rs.wasNull())
								pstm1.setDouble(25, anticipo_cuota);
							else
								pstm1.setObject(25, null);
							pstm1.setDouble(26, rs.getDouble("asignado"));
							pstm1.setDouble(27, rs.getDouble("vigente"));
							pstm1.addBatch();
							rows++;
							if((rows % 10000) == 0)
								pstm1.executeBatch();
						}
						CLogger.writeConsole("Records escritos: "+rows+" - mes: "+i);
						pstm1.executeBatch();
						rows_total += rows;
						rows=0;
						rs.close();
						pstm.close();
					}
					
					CLogger.writeConsole("Records escritos Totales: "+rows_total);
					pstm1.close();
					
					CLogger.writeConsole("Cargando datos a cache de MV_EJECUCION_PRESUPUESTARIA_GEOGRAFICO");
					ret = true;
					rows = 0;
					first=true;
					pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ejecucion_presupuestaria_geografico(ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, " + 
							"proyecto, actividad, obra, renglon, subgrupo, grupo," + 
							"fuente, geografico, ano_actual, asignado, vigente) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?) ");
					for(int i=1; i<13; i++){
						pstm = conn.prepareStatement("SELECT * FROM dashboard_historia.mv_ejecucion_presupuestaria_geografico where mes = ? and ejercicio between ? and ? ");
						pstm.setInt(1, i);
						pstm.setInt(2, ejercicio_inicio);
						pstm.setInt(3, ejercicio_fin);
						pstm.setFetchSize(10000);
						ResultSet rs = pstm.executeQuery();
						while(rs!=null && rs.next()){
							if(first){
								PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ejecucion_presupuestaria_geografico where ejercicio = ? ");
								pstm2.setInt(1, ejercicio_inicio);
								pstm2.setInt(1, ejercicio_fin);
								if (pstm2.executeUpdate()>0)
									CLogger.writeConsole("Registros eliminados");
								else
									CLogger.writeConsole("Sin registros para eliminar");	
								pstm2.close();
								first=false;
							}
							pstm1.setInt(1, rs.getInt("ejercicio"));
							pstm1.setInt(2, rs.getInt("mes"));
							pstm1.setInt(3, rs.getInt("entidad"));
							pstm1.setInt(4, rs.getInt("unidad_ejecutora"));
							pstm1.setInt(5, rs.getInt("programa"));
							pstm1.setInt(6, rs.getInt("subprograma"));
							pstm1.setInt(7, rs.getInt("proyecto"));
							pstm1.setInt(8, rs.getInt("actividad"));
							pstm1.setInt(9, rs.getInt("obra"));
							pstm1.setInt(10, rs.getInt("renglon"));
							pstm1.setInt(11, rs.getInt("subgrupo"));
							pstm1.setInt(12, rs.getInt("grupo"));
							pstm1.setInt(13, rs.getInt("fuente"));
							pstm1.setInt(14, rs.getInt("geografico"));
							pstm1.setDouble(15, rs.getDouble("ano_actual"));
							pstm1.setDouble(16, rs.getDouble("asignado"));
							pstm1.setDouble(17, rs.getDouble("vigente"));
							pstm1.addBatch();
							rows++;
							if((rows % 10000) == 0)
								pstm1.executeBatch();
						}
						CLogger.writeConsole("Records escritos: "+rows+" - mes: "+i);
						pstm1.executeBatch();
						rows_total += rows;
						rows=0;
						rs.close();
						pstm.close();
					}
					
					CLogger.writeConsole("Records escritos Totales: "+rows_total);
					pstm1.close();
					
					CLogger.writeConsole("Cargando datos a cache de MV_ESTRUCTURA");
					ret = true;
					rows = 0;
					first=true;
					pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_estructura(ejercicio, entidad, entidad_nombre, sigla, unidad_ejecutora, unidad_ejecutora_nombre, "
							+ "programa, programa_nombre, subprograma, subprograma_nombre, proyecto, proyecto_nombre, "
							+ "actividad, obra, actividad_obra_nombre) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?) ");
					pstm = conn.prepareStatement("SELECT * FROM dashboard_historia.mv_estructura where ejercicio between ? and ? ");
					pstm.setInt(1, ejercicio_inicio);
					pstm.setInt(2, ejercicio_fin);
					pstm.setFetchSize(10000);
					ResultSet rs = pstm.executeQuery();
					while(rs!=null && rs.next()){
						if(first){
							PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_estructura where ejercicio between ? and ? ");
							pstm2.setInt(1, ejercicio_inicio);
							pstm2.setInt(2, ejercicio_fin);
							if (pstm2.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm2.close();
							first=false;
						}
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("entidad"));
						pstm1.setString(3, rs.getString("entidad_nombre"));
						pstm1.setString(4, rs.getString("sigla"));
						pstm1.setInt(5, rs.getInt("unidad_ejecutora"));
						pstm1.setString(6, rs.getString("unidad_ejecutora_nombre"));
						pstm1.setInt(7, rs.getInt("programa"));
						pstm1.setString(8, rs.getString("programa_nombre"));
						pstm1.setInt(9, rs.getInt("subprograma"));
						pstm1.setString(10, rs.getString("subprograma_nombre"));
						pstm1.setInt(11, rs.getInt("proyecto"));
						pstm1.setString(12, rs.getString("proyecto_nombre"));
						pstm1.setInt(13, rs.getInt("actividad"));
						pstm1.setInt(14, rs.getInt("obra"));
						pstm1.setString(15, rs.getString("actividad_obra_nombre"));
						pstm1.addBatch();
						rows++;
						if((rows % 10000) == 0)
							pstm1.executeBatch();
					}
					pstm1.executeBatch();
					rs.close();
					pstm.close();
					
					CLogger.writeConsole("Cargando datos a cache de MV_EJECUCION_PRESUPUESTARIA_MENSUALIZADA");
					ret = true;
					rows = 0;
					first=true;
					pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ejecucion_presupuestaria_mensualizada(ejercicio, entidad, unidad_ejecutora, "
							+ "programa, subprograma, proyecto,actividad, obra, fuente, renglon, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12) "
							+ "values (?,?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,?) ");
					pstm = conn.prepareStatement("SELECT * FROM dashboard_historia.mv_ejecucion_presupuestaria_mensualizada where ejercicio between ? and ? ");
					pstm.setInt(1, ejercicio_inicio);
					pstm.setInt(2, ejercicio_fin);
					pstm.setFetchSize(10000);
					rs = pstm.executeQuery();
					while(rs!=null && rs.next()){
						if(first){
							PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ejecucion_presupuestaria_mensualizada where ejercicio between ? and ? ");
							pstm2.setInt(1, ejercicio_inicio);
							pstm2.setInt(2, ejercicio_fin);
							if (pstm2.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm2.close();
							first=false;
						}
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("entidad"));
						pstm1.setInt(3, rs.getInt("unidad_ejecutora"));
						pstm1.setInt(4, rs.getInt("programa"));
						pstm1.setInt(5, rs.getInt("subprograma"));
						pstm1.setInt(6, rs.getInt("proyecto"));
						pstm1.setInt(7, rs.getInt("actividad"));
						pstm1.setInt(8, rs.getInt("obra"));
						pstm1.setInt(9, rs.getInt("fuente"));
						pstm1.setInt(10, rs.getInt("renglon"));
						pstm1.setDouble(11, rs.getDouble("m1"));
						pstm1.setDouble(12, rs.getDouble("m2"));
						pstm1.setDouble(13, rs.getDouble("m3"));
						pstm1.setDouble(14, rs.getDouble("m4"));
						pstm1.setDouble(15, rs.getDouble("m5"));
						pstm1.setDouble(16, rs.getDouble("m6"));
						pstm1.setDouble(17, rs.getDouble("m7"));
						pstm1.setDouble(18, rs.getDouble("m8"));
						pstm1.setDouble(19, rs.getDouble("m9"));
						pstm1.setDouble(20, rs.getDouble("m10"));
						pstm1.setDouble(21, rs.getDouble("m11"));
						pstm1.setDouble(22, rs.getDouble("m12"));
						pstm1.addBatch();
						rows++;
						if((rows % 10000) == 0)
							pstm1.executeBatch();
					}
					pstm1.executeBatch();
					rs.close();
					pstm.close();
					
					
					
					CLogger.writeConsole("Records escritos Totales: "+rows);
					pstm1.close(); 
				}
				
			}					
				
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 2: CEjecucionPresupuestaria.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
