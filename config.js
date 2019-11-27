const Pulsar = require('pulsar-client');

const auth = new Pulsar.AuthenticationToken({
	token: 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjQ3Mjk1ODUzNDQsImlhdCI6MTU3MzgyNTM0NCwiaXNzIjoiY29tLm92aC5pby5zdHJlYW0uU2FuY3R1YXJ5IiwibmJmIjoxNTczODI1MzQ0LCJzdWIiOiJmZTZkY2EwYmI1OTI0MmY5OWRiMTVlNGFmZWRlMTllNS5icm93c2luZy45MWY5ODZjMi1iOGYwLTQzM2EtYmJmZS1mZGMzNDE5ODk3MzUifQ.adggkQE9jVNaBSIyIx389tR8kwgWQkWDQwRKp4I1lk8FmGQ2-MZdA90c9haK_OiFiWxGyvjrOP0_Qq0DuCVKGLD4YNWaNjxc_pe7uPp4-tbk3G944yFBXch9ZWzNCJSLlrZgDABgRZDqr607e0qN-UZjlOm06V0d8hmVJc3i0Pa_Iz6yjU58XszMfxSG5J5rCbH8V4B-NSoXgyO6FerCMuBybNigAJwZHPglemFZ27htksdu6gygJXOmxys1kVJaw4e1b3CT8bThcw2Gr0Z_POQMvmHByLD9OpChOrGUhs6JqjJ6YFxwPAvnrSbShad9zhJq53WabE4r4yAMEmqXxw'
});
   
console.log('token', auth);
console.log(auth);

const client = await new Pulsar.Client({
   serviceUrl: 'pulsar+ssl://gra.stream.io.ovh.net:6650',
   authentication: auth,
   operationTimeoutSeconds: 30
 });

// Create a producer
// console.log('client', client); 
// console.log( client); 
 
	 
 // Create a consumer
 const consumer = await client.subscribe({
	topic: 'persistent://fe6dca0bb59242f99db15e4afede19e5/browsing/browsing', 
	subscription: 'sub1',
	subscriptionType: 'Shared',
 });

 module.exports = consumer;