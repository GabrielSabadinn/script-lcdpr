const http = require('http');
const fastify = require('fastify')();
const mssql = require('mssql');
const axios = require('axios');
const fs = require('fs');

let server;

const serverFactory = (handler, opts) => {
  server = http.createServer((req, res) => {
    handler(req, res);
  });

  return server;
};

fastify.register((fastify, opts, next) => {
  mssql.connect(process.env.SQLServer)
    .then(() => {
      console.log('Conexão com o banco de dados estabelecida com sucesso');
      next();
    })
    .catch((error) => {
      console.error('Erro ao conectar ao banco de dados:', error);
      process.exit(1);
    });
});



class Registro0000 {
    constructor(data) {
        this.REG = "0000";
        this.NOME_ESC = "LCDPR";
        this.COD_VER = data.COD_VER;
        this.CPF = data.CPF;
        this.NOME = data.NOME;
        this.IND_SIT_INI_PER = data.IND_SIT_INI_PER;
        this.SIT_ESPECIAL = data.SIT_ESPECIAL;
        this.DT_SIT_ESP = data.DT_SIT_ESP || null;
        this.DT_INI = data.DT_INI;
        this.DT_FIN = data.DT_FIN;
    }

  

    validar() {
     if (!this.COD_VER || typeof this.COD_VER !== 'string'){
        return false;
     }
     if (!this.CPF || typeof this.CPF !== 'string'){
        return false;
     }
     if (!this.NOME || typeof this.NOME !== 'string'){
        return false;
     }
     if (!this.IND_SIT_INI_PER || !['0', '1', '2'].includes(this.IND_SIT_INI_PER)){
        return false;
     }
     if (!this.SIT_ESPECIAL || !['0', '1', '2', '3'].includes(this.SIT_ESPECIAL)){
        return false;
     }
     if (!this.DT_INI || typeof this.DT_INI !== 'string' || this.DT_INI.length !== 8){
        return false;
     }
     let a = typeof this.DT_FIN
  
     if (!this.DT_FIN ||  a !== 'string' || this.DT_FIN.length !== 8){
        return false;
    }
    return true;
  }
}
  
class Registro0010{
    constructor(data){
        this.REG = "0010";
        this.FORMA_APUR = data.FORMA_APUR;
    }
    validar(){
        if(!this.FORMA_APUR || !['1', '2'].includes(this.FORMA_APUR)){
            return false;
        }
        return true;
    }
}

 class Registro0030{
    constructor(data){
        this.REG = "0030";
        this.ENDERECO = data.ENDERECO;
        this.NUM = data.NUM;
        this.COMPL = data.COMPL || null;
        this.BAIRRO = data.BAIRRO; 
        this.UF = data.UF;
        this.COD_MUN = data.COD_MUN;
        this.CEP = data.CEP;
        this.NUM_TEL = data.NUM_TEL || null;
        this.EMAIL = data.EMAIL;
    }
    async validar(){
        if(
            !this.ENDERECO ||
            !this.NUM || 
            !this.BAIRRO ||
            !this.UF ||
            !this.COD_MUN ||
            !this.CEP ||
            !this.EMAIL 
        ){
            return false;
        }
        if(!['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'].includes(this.UF)) {
            return false;
    }
    try {
        const response = await axios.get(`http://viacep.com.br/ws/${this.CEP}/json/`);
        if (response.status === 200 && response.data.ibge) {
          if (response.data.ibge !== this.COD_MUN) {
            return false; // O codigo do Município não corresponde ao encontrado na api do cep
          }
        } else {
          return false; // a api cep não retornou os dados esperados
        }
      } catch (error) {
        return false; // erro ao consultar a api do cep
      }
  
      return true; // se todas as validações passaram, retorna true
    }
  }
 
  class Registro0045{
    constructor(data){
        this.REG = "0045";
        this.COD_IMOVEL = data.COD_IMOVEL;
        this.TIPO_CONTRAPARTE = data.TIPO_CONTRAPARTE; 
        this.ID_CONTRAPARTE = data.ID_CONTRAPARTE;
        this.NOME_CONTRAPARTE = data.NOME_CONTRAPARTE;
        this.PERC_CONTRAPARTE = data.PERC_CONTRAPARTE;
    }
    validar(){
        if(
            !this.COD_IMOVEL ||
            !this.TIPO_CONTRAPARTE ||
            !this.ID_CONTRAPARTE || 
            !this.NOME_CONTRAPARTE ||
            !this.PERC_CONTRAPARTE
        ){
            return false;
        }
        if(!['1', '2', '3', '4', '5']. includes (this.TIPO_CONTRAPARTE)){
            return false;
        }
        if (!this.ID_CONTRAPARTE.match(/^\d{11}$/) && !this.ID_CONTRAPARTE.match(/^\d{14}$/)){
            return false;
        }
        if(this.NOME_CONTRAPARTE.length > 50){
            return false;
        }
        if(isNaN(this.PERC_CONTRAPARTE) || this.PERC_CONTRAPARTE < 0 || this.PERC_CONTRAPARTE > 100){
            return false;
        }
        return true;
    }
  }

  class Registro0040{
    constructor(data){
        this.REG = "0040";
        this.COD_IMOVEL = data.COD_IMOVEL;
        this.PAIS = data.PAIS;
        this.MOEDA = "BRL";
        this.CAD_ITR = data.CAD_ITR || null;
        this.CAEPF = data.CAEPF || null;
        this.INSCR_ESTADUAL = data.INSCR_ESTADUAL || null;
        this.NOME_IMOVEL = data.NOME_IMOVEL;
        this.ENDERECO = data.ENDERECO;
        this.NUM = data.NUM || null;
        this.COMPL = data.COMPL || null;
        this.BAIRRO = data.BAIRRO;
        this.UF = data.UF;
        this.COD_MUN = data.COD_MUN || null;
        this.CEP = data.CEP || null;
        this.TIPO_EXPLORACAO = data.TIPO_EXPLORACAO;
        this.PARTICIPACAO = data.PARTICIPACAO;
    }
    async validar(){
        if(
            !this.COD_IMOVEL ||
            !this.PAIS ||
            !this.NOME_IMOVEL ||
            !this.ENDERECO ||
            !this.BAIRRO ||
            !this.UF ||
            !this.TIPO_EXPLORACAO ||
            !this.PARTICIPACAO 
        ){
            return false;
        }
        if(this.PAIS === "BR"){
            if(!this.CAD_ITR || !this.CAEPF){
                return false;
            }
        }else{
            if(!this.CAD_ITR || this.CAEPF){
                return false;
            }
        }
        if (!['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'].includes(this.UF)) {
            return false;
          }
          if (this.COD_MUN && this.PAIS === "BR") {
            try {
              const response = await axios.get(`http://viacep.com.br/ws/${this.CEP}/json/`);
              if (response.status === 200 && response.data.ibge) {
                if (response.data.ibge !== this.COD_MUN) {
                  return false; // o cod do municipio nao corresponde ao encontrado na API 
                }
              } else {
                return false; // A API  nao retornou os dados corretos
              }
            } catch (error) {
              return false; // erro ao consultar a API 
            }
          }
      
          return true; // Se todas as validações passaram, retorna true
        }
      }

    class Regristro0050{
        constructor(data){
            this.REG = "0050";
            this.COD_CONTA = data.COD_CONTA;
            this.PAIS_CTA = data.PAIS_CTA;
            this.BANCO = data.BANCO || null;
            this.NOME_BANCO = data.NOME_BANCO || null;
            this.AGENCIA = data.AGENCIA || null;
            this.NUM_CONTA = data.NUM_CONTA || null;
        }
        validar(){
            const paisesValidos = [
                "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AN", "AO", "AQ", "AR", "AS", "AT", "AU", "AW", "AX", "AZ",
                "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BQ", "BR", "BS", "BT", "BV", "BW", "BY", "BZ",
                "CA", "CC", "CD", "CF", "CG", "CH", "CI", "CK", "CL", "CM", "CN", "CO", "CR", "CU", "CV", "CW", "CX", "CY", "CZ",
                "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE", "EG", "EH", "ER", "ES", "ET",
                "FI", "FJ", "FK", "FM", "FO", "FR",
                "GA", "GB", "GD", "GE", "GF", "GG", "GH", "GI", "GL", "GM", "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW", "GY","HK", "HM", "HN", "HR", "HT", "HU",
                "ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IR", "IS", "IT","JE", "JM", "JO", "JP",
                "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KW", "KY", "KZ",
                "LA", "LB", "LC", "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY",
                "MA", "MC", "MD", "ME", "MF", "MG", "MH", "MK", "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ",
                "NA", "NC", "NE", "NF", "NG", "NI", "NL", "NO", "NP", "NR", "NU", "NZ","OM",
                "PA", "PE", "PF", "PG", "PH", "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PW", "PY","QA","RE", "RO", "RS", "RU", "RW",
                "SA", "SB", "SC", "SD", "SE", "SG", "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS", "ST", "SV", "SX", "SY", "SZ",
                "TC", "TD", "TF", "TG", "TH", "TJ", "TK", "TL", "TM", "TN", "TO", "TR", "TT", "TV", "TW", "TZ",
                "UA", "UG", "UM", "US", "UY", "UZ","VA", "VC", "VE", "VG", "VI", "VN", "VU", "WF", "WS","YE", "YT","ZA", "ZM", "ZW"
            ];
            if(!this.COD_CONTA || !this.PAIS_CTA){
                return false;
            }
            if(!paisesValidos.includes(this.PAIS_CTA)){
                return false;
            }
            if(!this.PAIS_CTA === "BR"){
                if(!this.BANCO || !this.NOME_BANCO || this.AGENCIA || !this.NUM_CONTA){
                    return false;
                }
            
            }
            return true; // se todas validações passaram 
        }
    }

    class RegistroQ100 {
      constructor(data) {
        this.REG = "Q100";
        this.DATA = data.DATA;
        this.COD_IMOVEL = data.COD_IMOVEL;
        this.COD_CONTA = data.COD_CONTA;
        this.NUM_DOC = data.NUM_DOC || null;
        this.TIPO_DOC = data.TIPO_DOC;
        this.HIST = data.HIST || null;
        this.ID_PARTIC = data.ID_PARTIC;
        this.TIPO_LANC = data.TIPO_LANC;
        this.VL_ENTRADA = data.VL_ENTRADA;
        this.VL_SAIDA = data.VL_SAIDA;
        this.SLD_FIN = data.SLD_FIN;
        this.NAT_SLD_FIN = data.NAT_SLD_FIN !== undefined ? data.NAT_SLD_FIN : null;
      }
  
      validar() {
          if (
              !this.DATA ||
              !this.COD_IMOVEL ||
              !this.COD_CONTA ||
              !this.TIPO_DOC ||
              !this.ID_PARTIC ||
              (this.TIPO_LANC !== "1" && this.TIPO_LANC !== "2" && this.TIPO_LANC !== "3") ||
              isNaN(this.VL_ENTRADA) ||
              isNaN(this.VL_SAIDA) ||
              isNaN(this.SLD_FIN) ||
              (this.NAT_SLD_FIN && this.NAT_SLD_FIN !== "N" && this.NAT_SLD_FIN !== "P")
          ) {
              return false;
          }
  
          if (this.TIPO_LANC === "1" && !this.VL_ENTRADA) {
              return false;
          }
  
          if (this.TIPO_LANC === "2" && !this.VL_SAIDA) {
              return false;
          }
  
          if (this.TIPO_LANC === "3" && (!this.VL_ENTRADA || !this.VL_SAIDA)) {
              return false;
          }
  
          if (this.SLD_FIN !== null && this.NAT_SLD_FIN === null) {
              return false;
          }
  
          return true;
      }
  }
  

      class RegistroQ200 {
        constructor(data) {
          this.REG = "Q200";
          this.MES = data.MES;
          this.VL_ENTRADA = data.VL_ENTRADA.toFixed(0); 
          this.VL_SAIDA = data.VL_SAIDA.toFixed(0); 
          this.SLD_FIN = data.SLD_FIN.toFixed(0); 
          this.NAT_SLD_FIN = data.NAT_SLD_FIN;
        }
      }
    
      class GeradorRegistroQ200 {
        constructor(listaRegistroQ100, registro0000) {
            this.listaRegistroQ100 = listaRegistroQ100;
            this.registro0000 = registro0000;
        }
        gerarRegistroQ200PorMes(mes) {
          let totalVLEntrada = 0;
          let totalVLSaida = 0;
          let saldoAcumulado = 0; 
          let year = null;
      
          // Pegando o ano diretamente do primeiro registro Q100 que encontrarmos
          if (!year && this.listaRegistroQ100 && this.listaRegistroQ100.length > 0) {
              year = this.listaRegistroQ100[0].DATA.slice(4, 8);
          }
      
          // Processando apenas os registros Q100 do mês em questão
          for (let registro of this.listaRegistroQ100) {
              let mesRegistro = registro.DATA.substring(2, 4);
              if (mesRegistro === mes) {
                  totalVLEntrada += Number(registro.VL_ENTRADA);
                  totalVLSaida += Number(registro.VL_SAIDA);
              }
          }
      
          // O saldo do mês é o saldo anterior mais total de entradas menos o total de saídas
          saldoAcumulado += totalVLEntrada - totalVLSaida;
      
          let NAT_SLD_FIN = saldoAcumulado >= 0 ? "P" : "N";  // Determinando o NAT_SLD_FIN
      
          return {
              MES: `${mes}${year || ''}`,  // Aqui estamos usando o ano que pegamos dos registros Q100
              VL_ENTRADA: totalVLEntrada,
              VL_SAIDA: totalVLSaida,
              SLD_FIN: saldoAcumulado,
              NAT_SLD_FIN: NAT_SLD_FIN,
          };
      }
    }
    
  
class Registro9999 {
    constructor(data) {
      this.REG = "9999";
      this.IDENT_NOM = data.IDENT_NOM || null;
      this.IDENT_CPF_CNPJ = data.IDENT_CPF_CNPJ || null;
      this.IND_CRC = data.IND_CRC || null;
      this.EMAIL = data.EMAIL || null;
      this.FONE = data.FONE || null;
      this.QTD_LIN = data.QTD_LIN;
    }
  
    validar() {
      if (!this.IDENT_NOM || !this.EMAIL || !this.FONE || !this.QTD_LIN) {
        return false;
      }
      if (!this.validarCPF_CNPJ()) {
        return false;
      }
      if (!this.validarEmail()) {
        return false;
      }
      if (this.QTD_LIN < 1 || isNaN(this.QTD_LIN)) {
        return false;
      }
  
      return true;
    }
  
    validarCPF_CNPJ() {
      if (this.IDENT_CPF_CNPJ.length === 11) {
        return true;
      } else if (this.IDENT_CPF_CNPJ.length === 14) {
        return true;
      }
      return false;
    }
  
    validarEmail() {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      return emailRegex.test(this.EMAIL);
    }
  }

  function convertPercentageToString(percentage) {
    const cleanedPercentage = percentage.replace('%', '');
    const number = parseFloat(cleanedPercentage) * 100;
    const formattedNumber = number.toFixed(2);
    const parts = formattedNumber.split('.');
    const integerPart = parts[0];
    const decimalPart = parts[1].padStart(2, '0');
    return integerPart + decimalPart;
  }
  
  function formatDateToText(date) {
    const day = date.getDate().toString().padStart(2, '0');
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    const year = date.getFullYear().toString();
    return `${day}${month}${year}`;
  }
  
  function formatResultsToText(results) {
    let formattedText = '';
  
    for (const key in results) {
      if (typeof results[key] === 'boolean' && results[key]) {
        // Lidar com valores booleanos, se necessário
      } else {
        for (let n in results[key]) {
          for (let a in results[key][n]) {
            const value = results[key][n][a];
            if (value !== null) {
              if (typeof value === 'string') {
                if (value.includes('.')) {
                  const [integerPart, decimalPart] = value.split('.');
                  const formattedValue = integerPart.padStart(3, '0') + decimalPart.padEnd(2, '0');
                  formattedText += formattedValue + '|';
                } else if (value.endsWith('%')) {
                  formattedText += convertPercentageToString(value) + '|';
                } else if (/^\d{2}\/\d{2}\/\d{4}$/.test(value)) {
                  const [day, month, year] = value.split('/');
                  const formattedDate = formatDateToText(new Date(`${year}-${month}-${day}`));
                  formattedText += formattedDate + '|';
                } else {
                  formattedText += value.replace('|', '') + '|';
                }
              } else if (typeof value === 'number') {
                const stringValue = String(value.toFixed(2));
                const [integerPart, decimalPart] = stringValue.split('.');
                const formattedValue = integerPart.padStart(5, '0') + decimalPart.padEnd(2, '0');
                formattedText += formattedValue + '|';
              } else {
                formattedText += value + '|';
              }
            } else {
              formattedText += '|';
            }
          }
          formattedText += '\n';
        }
      }
    }
    return formattedText;
  }
  

  fastify.post('/api/lcdpr/registro', async (requisicao, resposta) => {
    const dados = requisicao.body;

   
    const registro0000s = dados.registro0000;
    const registros0000 = registro0000s.map(data => new Registro0000(data));
    const registro0010s = dados.registro0010;
    const registros0010 = registro0010s.map(data => new Registro0010(data));
    const registro0030s = dados.registro0030;
    const registros0030 = registro0030s.map(data => new Registro0030(data));
    const registro0040s = dados.registro0040;
    const registros0040 = registro0040s.map(data => new Registro0040(data));
    const registro0045s = dados.registro0045;
    const registros0045 = registro0045s.map(data => new Registro0045(data));
    const registro0050s = dados.registro0050;
    const registros0050 = registro0050s.map(data => new Regristro0050(data));
    const registroQ100s = dados.registroQ100;
    if (!registroQ100s) {
        resposta.code(400).send({ error: "registroQ100s não fornecido na requisição" });
        return;
    }
 
const registrosQ100 = registroQ100s
.map(data => new RegistroQ100(data))
.sort((a, b) => new Date(a.DATA) - new Date(b.DATA));
console.log("Registros Q100 processados:", registrosQ100);


const geradorQ200 = new GeradorRegistroQ200(registrosQ100, dados.registro0000);

const registrosQ200 = [];
let saldoAcumuladoDoMesAnterior = 0; 
for (let mes = 1; mes <= 12; mes++) {
  let mesFormatado = String(mes).padStart(2, '0');
  const registroQ200 = geradorQ200.gerarRegistroQ200PorMes(mesFormatado, saldoAcumuladoDoMesAnterior);
  saldoAcumuladoDoMesAnterior = registroQ200.SLD_FIN; 
  if (registroQ200.VL_ENTRADA || registroQ200.VL_SAIDA) {
      registrosQ200.push(new RegistroQ200(registroQ200));
  }
}

console.log("Registros Q200 finais:", registrosQ200);

    const registro9999s = dados.registro9999;
    const registros9999 = registro9999s.map(data => new Registro9999(data));

    const resultados = {
        registro0000: registros0000,
        registro0010: registros0010,
        registro0030: registros0030,
        registro0040: registros0040,
        registro0045: registros0045,
        registro0050: registros0050,
        registroQ100: registrosQ100,
        registroQ200: registrosQ200,
        registro9999: registros9999,
    };
    console.log("Dados brutos antes da formatação:", resultados);
    
    await insertIntoDatabase(dados);

    const formattedText = await formatResultsToText(resultados);


    
    fs.writeFileSync('registros.txt', formattedText, 'utf-8');
    console.log('Arquivo registros.txt criado com sucesso.');

    resposta
    .code(200)
    .header('Content-Type', 'text/plain; charset=utf-8')
    .header('Content-Disposition', 'attachment; filename=resultados.txt')
    .send(formattedText);
});
async function insertIntoDatabase(data) {
  const pool = await mssql.connect(process.env.SQLServer);
  const request = new mssql.Request(pool);
  
  // Convertendo o objeto data em uma string JSON
  const jsonString = JSON.stringify(data);

  // Definindo os parâmetros para a consulta
  request.input('cliente', mssql.Int, 0);
  request.input('json', mssql.NVarChar(mssql.MAX), jsonString);

  // Inserindo jsonString e o clienteId (definido como 0) na tabela Registro
  await request.query(`INSERT INTO [LCDPR].[Registro] (cliente, json) VALUES (@cliente, @json)`);
}

  const portNumber = Number(process.env.PORT || 3001);
  
  const start = async () => {
    try {
      await fastify.listen({ host: '0.0.0.0', ...(isNaN(portNumber) ? { path: process.env.PORT } : { port: portNumber }) });
      console.log(`Server is running on port ${fastify.server.address().port}`);
    } catch (err) {
      fastify.log.error(err);
      process.exit(1);
    }
  };
  
  start();
  