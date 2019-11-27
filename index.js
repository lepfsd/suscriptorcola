const psaux = require('psaux');

const consumer = require('./config');
const crear_fichero = require('./services/crearFichero');

(async () => {
    
	console.log('nprocesospre', 0 ) 
	nprocesos=await  psaux().then(list => {  
		let chrome = list.query({command: '~suscriptorcola.js'});
		if (chrome) {
			console.log('Chrome process found!', chrome.length);
		}
		return chrome.length;
		//resolve( chrome.length);
	}); 

	console.log('nprocesos', nprocesos ); 
	if (nprocesos>4) {  
		return process.exit(22);
	 }	

	//console.log( 'pre crear'); 
	// Receive messages
	while( await crear_fichero.crear(consumer, 50000)){

	}
	//  const creafiles =   await crearfichero(consumer, 15);
	console.log( 'post crear'); 

	await consumer.close();
})().catch(e => {
		console.log('error', e);
	}
);