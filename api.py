#Fabio Leandro Lapuinka 02/09/2025 
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import threading
import time
import schedule
import json

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///protheus_integration.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Modelos das tabelas muro
class SZ0(db.Model):
    __tablename__ = 'SZ0'
    id = db.Column(db.Integer, primary_key=True)
    ZO_FILIAL = db.Column(db.String(10))
    ZO_DATA = db.Column(db.DateTime, default=datetime.utcnow)
    ZO_TPREG = db.Column(db.String(1))  # I=Inclusão, A=Alteração, E=Exclusão
    ZO_TPVEN = db.Column(db.String(1))  # A=Assinatura, C=Contrato Serviço, V=Venda Direta
    ZO_TIPO = db.Column(db.String(1))   # 1-9 conforme documentação
    ZO_NUMLAB = db.Column(db.String(20))
    ZO_NUMPRT = db.Column(db.String(20))
    ZO_CNPJ1 = db.Column(db.String(20))
    ZO_CNPJ2 = db.Column(db.String(20))
    ZO_CONPAG1 = db.Column(db.String(10))
    ZO_INICIO = db.Column(db.DateTime)
    ZO_UNVIG = db.Column(db.String(1))  # 1=Dia, 2=Meses, 3=Anos, 4=Indeterminado
    ZO_VIGE = db.Column(db.Integer)
    ZO_REAJ = db.Column(db.String(1))   # 1=Sim, 2=Não
    ZO_INDICE = db.Column(db.String(20))
    ZO_STATUS = db.Column(db.String(1))  # P=Processado, F=Falha, None=Em processamento
    ZO_MSGPROC = db.Column(db.Text)

class SZ1(db.Model):
    __tablename__ = 'SZ1'
    id = db.Column(db.Integer, primary_key=True)
    Z1_FILIAL = db.Column(db.String(10))
    Z1_NUMPRT = db.Column(db.String(20))
    Z1_PROD = db.Column(db.String(30))
    Z1_QUANT = db.Column(db.Numeric(15, 2))
    Z1_VALOR = db.Column(db.Numeric(15, 2))
    Z1_CC = db.Column(db.String(10))
    Z1_ITCONT = db.Column(db.String(10))
    Z1_CONTAB = db.Column(db.String(10))
    Z1_CLAVLR = db.Column(db.String(10))
    Z1_STATUS = db.Column(db.String(1))
    Z1_MSGPROC = db.Column(db.Text)

class SZ2(db.Model):
    __tablename__ = 'SZ2'
    id = db.Column(db.Integer, primary_key=True)
    Z2_FILIAL = db.Column(db.String(10))
    Z2_DATA = db.Column(db.DateTime, default=datetime.utcnow)
    Z2_TPREG = db.Column(db.String(1))
    Z2_COD = db.Column(db.String(10))
    Z2_LOJA = db.Column(db.String(10))
    Z2_CNPJ = db.Column(db.String(20))
    Z2_IE = db.Column(db.String(20))
    Z2_NOME = db.Column(db.String(100))
    Z2_END = db.Column(db.String(100))
    Z2_CEP = db.Column(db.String(10))
    Z2_BARRO = db.Column(db.String(50))
    Z2_COMPLEM = db.Column(db.String(50))
    Z2_TIPO = db.Column(db.String(1))
    Z2_EST = db.Column(db.String(2))
    Z2_MUN = db.Column(db.String(50))
    Z2_COD_MUN = db.Column(db.String(10))
    Z2_DDD = db.Column(db.String(3))
    Z2_TEL = db.Column(db.String(15))
    Z2_EMAIL = db.Column(db.String(100))
    Z2_CONTAT = db.Column(db.String(50))
    Z2_NATUREZ = db.Column(db.String(10))
    Z2_PESSOA = db.Column(db.String(1))
    Z2_PAIS = db.Column(db.String(3))
    Z2_CODPAIS = db.Column(db.String(5))
    Z2_STATUS = db.Column(db.String(1))
    Z2_MSGPROC = db.Column(db.Text)

class SZ3(db.Model):
    __tablename__ = 'SZ3'
    id = db.Column(db.Integer, primary_key=True)
    Z3_FILIAL = db.Column(db.String(10))
    Z3_DATA = db.Column(db.DateTime, default=datetime.utcnow)
    Z3_TPREG = db.Column(db.String(1))
    Z3_COD = db.Column(db.String(30))
    Z3_DESCR = db.Column(db.String(100))
    Z3_ISS = db.Column(db.Numeric(5, 2))
    Z3_TIPO = db.Column(db.String(2))  # SV, BN, EM
    Z3_UNID = db.Column(db.String(3))  # HR, KG, etc
    Z3_GRUPO = db.Column(db.String(10))
    Z3_NCM = db.Column(db.String(10))
    Z3_TRIBMUN = db.Column(db.String(10))
    Z3_TS = db.Column(db.String(10))
    Z3_ORIGEM = db.Column(db.String(1))  # 0=Nacional, 1=Importado
    Z3_STATUS = db.Column(db.String(1))
    Z3_MSGPROC = db.Column(db.Text)

# Endpoints da API
@app.route('/api/cliente', methods=['POST'])
def receber_cliente():
    try:
        data = request.get_json()
        
        # Validar dados obrigatórios
        if not data.get('document') or not data['document'].get('number'):
            return jsonify({'error': 'CNPJ/CPF é obrigatório'}), 400
        
        # Verificar se é inclusão, alteração ou exclusão
        # (implementar lógica baseada no documento existente)
        tp_reg = 'I'  # Inclusão por padrão
        
        cliente = SZ2(
            Z2_FILIAL=data.get('companyBRANCH', '01'),
            Z2_TPREG=tp_reg,
            Z2_COD='',  # Gerado posteriormente
            Z2_LOJA=data.get('companyBRANCH', '01'),
            Z2_CNPJ=data['document']['number'],
            Z2_IE=data.get('estadualInscription', ''),
            Z2_NOME=data.get('companyName', ''),
            Z2_END=data.get('address', [{}])[0].get('Endereço', ''),
            Z2_CEP=data.get('address', [{}])[0].get('CEP', ''),
            Z2_BARRO=data.get('address', [{}])[0].get('Bairro', ''),
            Z2_COMPLEM=data.get('address', [{}])[0].get('Complemento', ''),
            Z2_TIPO=data.get('Tipo', 'F'),  # F=Final, R=Revendedor
            Z2_EST=data.get('address', [{}])[0].get('UF', ''),
            Z2_MUN=data.get('address', [{}])[0].get('Estado', ''),
            Z2_COD_MUN='',  # Preencher com código do município
            Z2_DDD=data.get('communication', {}).get('phone', '')[:2] if data.get('communication', {}).get('phone') else '',
            Z2_TEL=data.get('communication', {}).get('phone', ''),
            Z2_EMAIL=data.get('communication', {}).get('email', ''),
            Z2_CONTAT=data.get('communication', {}).get('contactName', ''),
            Z2_NATUREZ=data.get('Nature', '0000000001'),
            Z2_PESSOA='J' if data['document'].get('type') == 'CNPJ' else 'F',
            Z2_PAIS='105',
            Z2_CODPAIS='01058',
            Z2_STATUS=None  # Em processamento
        )
        
        db.session.add(cliente)
        db.session.commit()
        
        return jsonify({'message': 'Cliente recebido com sucesso', 'id': cliente.id}), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/produto', methods=['POST'])
def receber_produto():
    try:
        data = request.get_json()
        
        if not data.get('Sku'):
            return jsonify({'error': 'SKU é obrigatório'}), 400
        
        produto = SZ3(
            Z3_FILIAL=data.get('Armazem', '01'),
            Z3_TPREG='I',  # Inclusão
            Z3_COD=data['Sku'],
            Z3_DESCR=data.get('descrição', ''),
            Z3_ISS=data.get('ISS', 0),
            Z3_TIPO='SV',  # Serviço por padrão
            Z3_UNID='HR',  # Horas por padrão
            Z3_GRUPO=data.get('Grupo', '0001'),
            Z3_NCM=data.get('Ncm', ''),
            Z3_TRIBMUN=data.get('codigo_tributacao_municipal', ''),
            Z3_TS=data.get('tipo_saida', ''),
            Z3_ORIGEM=data.get('Origem', '0'),
            Z3_STATUS=None
        )
        
        db.session.add(produto)
        db.session.commit()
        
        return jsonify({'message': 'Produto recebido com sucesso', 'id': produto.id}), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/pedido', methods=['POST'])
def receber_pedido():
    try:
        data = request.get_json()
        
        if not data.get('Pedido'):
            return jsonify({'error': 'Número do pedido é obrigatório'}), 400
        
        # Criar cabeçalho do pedido
        pedido = SZ0(
            ZO_FILIAL='01',
            ZO_TPREG='I',
            ZO_TPVEN=data.get('TipoVenda', 'V'),  # V=Venda direta por padrão
            ZO_TIPO=data.get('TipoFaturamento', '1'),
            ZO_NUMLAB=data['Pedido'],
            ZO_NUMPRT='',  # Será preenchido após processamento
            ZO_CNPJ1=data.get('Cliente1', {}).get('document', {}).get('number', ''),
            ZO_CNPJ2=data.get('Cliente2', {}).get('document', {}).get('number', ''),
            ZO_CONPAG1=data.get('PaymentTerms', '001'),
            ZO_INICIO=datetime.strptime(data.get('DataInicio', datetime.now().strftime('%Y-%m-%d')), '%Y-%m-%d'),
            ZO_UNVIG=data.get('UnidadeVigencia', '2'),  # Meses por padrão
            ZO_VIGE=data.get('Vigencia', 1),
            ZO_REAJ=data.get('Reajuste', '2'),  # Não por padrão
            ZO_INDICE=data.get('IndiceReajuste', ''),
            ZO_STATUS=None
        )
        
        db.session.add(pedido)
        db.session.flush()  # Para obter o ID
        
        # Criar itens do pedido
        for item in data.get('Itens', []):
            item_pedido = SZ1(
                Z1_FILIAL='01',
                Z1_NUMPRT=pedido.ZO_NUMPRT,  # Será atualizado
                Z1_PROD=item.get('Sku', ''),
                Z1_QUANT=item.get('QtdPV', 1),
                Z1_VALOR=item.get('PrcPV', 0),
                Z1_CC=item.get('CentroCusto', ''),
                Z1_ITCONT=item.get('ItemContabil', ''),
                Z1_CONTAB=item.get('ContaContabil', ''),
                Z1_CLAVLR=item.get('ClasseValor', ''),
                Z1_STATUS=None
            )
            db.session.add(item_pedido)
        
        db.session.commit()
        
        return jsonify({'message': 'Pedido recebido com sucesso', 'id': pedido.id}), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Rotinas de processamento agendado
def processar_cadastros():
    """Processa cadastros pendentes nas tabelas muro"""
    with app.app_context():
        # Processar clientes
        clientes_pendentes = SZ2.query.filter_by(Z2_STATUS=None).all()
        for cliente in clientes_pendentes:
            try:
                # Simular processamento do Protheus
                # Aqui viria a lógica real de integração com o Protheus
                cliente.Z2_STATUS = 'P'
                cliente.Z2_MSGPROC = 'Processado com sucesso'
                db.session.commit()
            except Exception as e:
                cliente.Z2_STATUS = 'F'
                cliente.Z2_MSGPROC = str(e)
                db.session.commit()
        
        # Processar produtos (similar ao acima)
        # Processar pedidos (similar ao acima)

def agendar_processamentos():
    """Configura os processamentos agendados"""
    schedule.every(5).minutes.do(processar_cadastros)
    schedule.every(10).minutes.do(processar_pedidos)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    
    # Iniciar thread para processamentos agendados
    scheduler_thread = threading.Thread(target=agendar_processamentos)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    
    app.run(debug=True, port=5000)
