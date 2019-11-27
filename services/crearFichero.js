var Url = require('url-parse');
const fs = require('fs');
const process = require('process');
const con = require('./database');
const filepath = '/functions/colas/files/';

async function crear(consumer, nlineas) {
	try {
	  var date1 = new Date();
 	  var filename = 'navegacion_'+process.pid+'_' ;  
	  let i = 1;
		   
	  for ( i = 0; i < nlineas; i += 1) {
  
		const separador='   '        
		const msg = await consumer.receive();
		let dd =  msg.getData().toString();	
		let aa=JSON.parse(dd);
		let fila = ''
		//  console.log('aaa' ,aa)
		console.log('*****************')
		// console.log(aa.recordbins)
		let pageviewurl = await new Url(aa.pageurl);
		pageviewurl.seccion=pageviewurl.pathname.split('/');
		pageviewurl.urlreal=pageviewurl.host+ pageviewurl.pathname ;
		let pageseccion =pageviewurl.seccion[1] || '/';
		//	console.log(pageviewurl);
		//redisclient.HINCRBY(["conteo:"+date1.getFullYear()+date1.getMonth()+date1.getDate()+date1.getHours(), "vozpopuli:hashtest 2", 1], redis.print);
		//redisclient.HINCRBY(["conteo:"+date1.getFullYear()+date1.getMonth()+date1.getDate()+date1.getHours(), "vozpopuli:hashtest 2", 1], redis.print );
		fila=aa.pageurl + separador + aa.title + separador+aa.documentreferal+separador+ aa.img+ separador+ pageviewurl.host+separador+pageseccion+'categoriacontenido'
		fila=fila+separador+ aa.pais+separador+ aa.citycode+ separador+ aa.city  + separador+ aa.asn+ aa.asnisp +aa.asnorg + separador+aa.UA
		fila=fila+separador+aa.ltm + separador + aa.clearbit
		fs.appendFileSync(filepath+filename+'.tmp'  , fila+'\n');
		
		if (aa.clearbit && aa.clearbit.match(/company/)) {
			let clearbit=JSON.parse(aa.clearbit);
			//&& aa.clearbit.company ) {	   
			
			if (clearbit.geoIP ) {
				delete clearbit['geoIP'] 
			} 
			if (clearbit.company && clearbit.company.site ) {
				delete clearbit['company']['site']; 
			} 
			if (clearbit.company && clearbit.company.description ) {
				delete clearbit['company']['description']; 
			} 
			if (clearbit.company && clearbit.company.timeZone ) {
				delete clearbit['company']['timeZone']; 
			} 
			if (clearbit.company && clearbit.company.utcOffset ) { 	 
				delete clearbit['company']['utcOffset']; 
			} 
			if (clearbit.company && clearbit.company.utcOffset ) { 	
				delete clearbit['company']['utcOffset'];
			 } 
			if (clearbit.company && clearbit.company.twitter ) { 
				delete clearbit['company']['twitter']['bio'] 
			} 
			
			//var sql = `INSERT IGNORE INTO companies (domain, domainmd5,cbid, data) VALUES ('${clearbit.domain}', MD5('${clearbit.domain}'), '${clearbit.company.id}', '${aa.clearbit}')`;
			var sql = `INSERT INTO companies (domain, domainmd5,cbid, hits, ip, cbkeys, data) VALUES ('${clearbit.domain}', MD5('${clearbit.domain}'), '${clearbit.company.id}',1, '${aa.realip}', '${JSON.stringify(Object.assign( Object.keys(clearbit), Object.keys(clearbit.company)) )}',
			JSON_QUOTE("${con.escape(JSON.stringify(clearbit))}")  ) ON DUPLICATE KEY UPDATE hits=hits+1 `; 	
			await  con.query(sql, function (err, result) {
				if (err) { 
					console.log("sql error", aa);
					console.log(sql);
					throw err;
				} 
			});
			
		}
		var ltm='{}';

		if ( aa.ltm && aa.ltm.match(/id/)) {
			ltm=JSON.parse(aa.ltm);
			ltm=JSON.stringify(ltm.Audience);
		}
		
		console.log('prepag', pageviewurl, pageviewurl.urlreal);
		var  sqlurl = `INSERT INTO urls (urlmd5,dominiomd5,  hits, url, title, imagen, seccion) VALUES ( MD5(?) , MD5('${pageviewurl.host}') ,  1, ? , ?, ?, ?  ) ON DUPLICATE KEY UPDATE hits=hits+1 `; 			
		await con.query(sqlurl, [pageviewurl.urlreal.toString(), pageviewurl.urlreal.toString(), aa.title,  aa.imagen , pageseccion ], function (err, result) {
			if (err) { 
				console.log(sqlurl,pageviewurl);
				console.log("sql error urls", err, aa);
				throw err;
			} 
		});
			
		await con.query(`INSERT INTO users (uuid, ltm, hits, pais, ispcode, ispstring, ispdomain, ip ) VALUES ('${aa.dmpid}', ?, 1, ?,'${aa.asn}', ? , ?, '${aa.realip}' ) ON DUPLICATE KEY UPDATE hits=hits+1 `, [ltm,aa.pais, aa.asnisp, aa.asnorg], function (err, result) {
			if (err) { 
				console.log("sql error users", err, aa);
				throw err;
			} 
		});

		//var  sqldomains = `INSERT INTO urls_users (uuid, urlmd5, dominiomd5, dominioseccionmd5) VALUES ('${aa.dmpid}',  MD5('${pageviewurl.urlreal}'),MD5('${pageviewurl.host}')  ,MD5('${pageviewurl.host+pageseccion}')   ) ON DUPLICATE KEY UPDATE  hits=hits+1 ;`; 
		await con.query(`INSERT INTO urls_users (uuid, urlmd5, dominiomd5, dominioseccionmd5, hits) VALUES ('${aa.dmpid}',  MD5('${pageviewurl.urlreal}'),MD5('${pageviewurl.host}')  ,MD5('${pageviewurl.host+pageseccion}') , 1  ) ON DUPLICATE KEY UPDATE  hits=hits+1 ;`, function (err, result) {
			if (err) { 	 
				console.log("sql error urls_users", err);
				throw err;
			} 
		});

		await   con.query( ` INSERT INTO dominio_users (uuid,   dominiomd5, hits ) VALUES ('${aa.dmpid}',  MD5('${pageviewurl.host}') , 1) ON DUPLICATE KEY UPDATE hits=hits+1 ;`, function (err, result) {
			if (err) { 		 
				console.log("sql error dominio_users", err);
				throw err;
			} 
		});

		if ((i%20)==0) {
				console.log('bb ', i );  
	
			console.log('pageviewurl', pageviewurl );
			console.log('pv' ,aa );
		// if (i>5000) { break; }
		}
		
		if ((i%100)==0 ) {
					
			var stats = fs.statSync(filepath+filename+'.tmp')              
			if ( stats.size > 10400000.0) { break; }
		}
		await  consumer.acknowledge(msg);
 	}

	fs.renameSync(filepath+filename+'.tmp', filepath+filename+'--'+i+'.txt'  , (err) => {
 		if (err) throw err;
		// console.log('Rename complete!');
		return process.exit(22);
		
	});
  
	console.log( 'salida');   
	return true;  
	} catch(e) {
		console.log(e)    
	}

}

module.exports = {
	crearfichero

} 