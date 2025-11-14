import pandas as pd
import os
import numpy as np
from .data_processing import DataProcessor
from .data_export import DataExporter

class IDHDataPipeline:
    def __init__(self):
        self.processor = DataProcessor()
        self.exporter = DataExporter()
        self.raw_path = "src/data/raw/"
        self.output_path = "src/data/output/"
    
    def run_kaggle_idh_analysis(self):
        """Pipeline ESPECÍFICO para o dataset Kaggle HDI Brazil"""
        print("🚀 Iniciando análise com dataset Kaggle HDI Brazil")
        
        # 1. Verificando se o arquivo correto está presente
        arquivo_encontrado = None
        for arquivo in os.listdir(self.raw_path):
            if arquivo.endswith('.csv'):
                arquivo_encontrado = arquivo
                break
        
        if not arquivo_encontrado:
            print("❌ Nenhum arquivo CSV encontrado na pasta raw/")
            return None
        
        print(f"✅ Usando arquivo: {arquivo_encontrado}")
        
        # 2. Carregando os dados
        try:
            df = pd.read_csv(os.path.join(self.raw_path, arquivo_encontrado))
            print(f"📊 Dataset carregado: {len(df)} registros, {len(df.columns)} colunas")
            
            # Mostrar colunas importantes que serão usadas
            print(f"🎯 Colunas principais identificadas:")
            print(f"   - IDH: idhm")
            print(f"   - Município: município") 
            print(f"   - Estado: uf")
            print(f"   - Ano: ano")
            
        except Exception as e:
            print(f"❌ Erro ao carregar arquivo: {e}")
            return None
        
        # 3. Processando dados de IDH
        print("🔄 Processando dados de IDH com estrutura real...")
        
        resultados = self._analisar_idh_estrutura_real(df)
        
        # 4. Exportando resultados
        print("💾 Exportando análises...")
        self._exportar_resultados_estrutura_real(resultados)
        
        print("✅ Análise de IDH concluída!")
        return resultados
    
    def _analisar_idh_estrutura_real(self, df):
        """Análises específicas para a estrutura REAL do dataset"""
        analises = {}
        
        # 1. Estatísticas básicas do IDH principal
        analises['estatisticas_gerais'] = {
            'media_idh': df['idhm'].mean(),
            'mediana_idh': df['idhm'].median(),
            'max_idh': df['idhm'].max(),
            'min_idh': df['idhm'].min(),
            'desvio_padrao': df['idhm'].std(),
            'total_municipios': len(df),
            'anos_analisados': df['ano'].unique().tolist()
        }
        
        # 2. Classificar por categorias de IDH
        df_classificado = df.copy()
        df_classificado['categoria_idh'] = df_classificado['idhm'].apply(self._classificar_categoria_idh)
        analises['categorias_idh'] = df_classificado['categoria_idh'].value_counts().to_dict()
        
        # 3. Top 10 municípios por IDH (priorizando ano mais recente)
        ano_mais_recente = df['ano'].max()
        df_recente = df[df['ano'] == ano_mais_recente]
        
        top_10 = df_recente.nlargest(10, 'idhm')[['município', 'uf', 'idhm', 'idhm_e', 'idhm_l', 'idhm_r']]
        analises['top_10_idh'] = top_10
        analises['ano_mais_recente'] = ano_mais_recente
        
        # 4. Análise por estado
        idh_por_estado = df_recente.groupby('uf')['idhm'].agg([
            'mean', 'std', 'count', 'min', 'max'
        ]).round(4)
        
        # Adicionando a região
        idh_por_estado['regiao'] = idh_por_estado.index.map(self._uf_para_regiao)
        analises['idh_por_estado'] = idh_por_estado
        
        # 5. Análise por região
        idh_por_regiao = idh_por_estado.groupby('regiao')['mean'].agg([
            'mean', 'std', 'count'
        ]).round(4)
        analises['idh_por_regiao'] = idh_por_regiao
        
        # 6. Evolução temporal do IDH médio
        evolucao_temporal = df.groupby('ano')['idhm'].agg(['mean', 'count']).round(4)
        analises['evolucao_temporal'] = evolucao_temporal
        
        # 7. Análise dos componentes do IDH (educação, longevidade, renda)
        componentes_analise = {
            'idhm_e_media': df_recente['idhm_e'].mean(),
            'idhm_l_media': df_recente['idhm_l'].mean(), 
            'idhm_r_media': df_recente['idhm_r'].mean(),
            'correlacao_educacao_renda': df_recente['idhm_e'].corr(df_recente['idhm_r']),
            'correlacao_longevidade_renda': df_recente['idhm_l'].corr(df_recente['idhm_r'])
        }
        analises['componentes_idh'] = componentes_analise
        
        return {
            'dataframe_original': df,
            'dataframe_recente': df_recente,
            'dataframe_classificado': df_classificado,
            'analises': analises
        }
    
    def _classificar_categoria_idh(self, valor_idh):
        """Classifica o IDH em categorias conforme a ONU"""
        if pd.isna(valor_idh):
            return 'Sem dados'
        elif valor_idh >= 0.800:
            return 'Muito Alto'
        elif valor_idh >= 0.700:
            return 'Alto' 
        elif valor_idh >= 0.550:
            return 'Médio'
        else:
            return 'Baixo'
    
    def _uf_para_regiao(self, uf):
        """Converte UF para região geográfica"""
        regioes = {
            'Norte': ['AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO'],
            'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
            'Centro-Oeste': ['DF', 'GO', 'MT', 'MS'],
            'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
            'Sul': ['PR', 'RS', 'SC']
        }
        
        for regiao, estados in regioes.items():
            if uf in estados:
                return regiao
        return 'Outra'
    
    def _exportar_resultados_estrutura_real(self, resultados):
        """Exporta resultados para a estrutura real do dataset"""
        analises = resultados['analises']
        
        # 1. Dataframe completo classificado
        self.exporter.to_csv(resultados['dataframe_classificado'], 'idh_brasil_classificado.csv')
        
        # 2. Dataframe do ano mais recente
        self.exporter.to_csv(resultados['dataframe_recente'], 'idh_ano_mais_recente.csv')
        
        # 3. Estatísticas gerais
        df_estatisticas = pd.DataFrame([analises['estatisticas_gerais']])
        self.exporter.to_csv(df_estatisticas, 'estatisticas_gerais_idh.csv')
        
        # 4. Distribuição por categorias
        df_categorias = pd.DataFrame(list(analises['categorias_idh'].items()), 
                                   columns=['Categoria', 'Quantidade'])
        self.exporter.to_csv(df_categorias, 'distribuicao_categorias_idh.csv')
        
        # 5. Top 10 municípios
        self.exporter.to_csv(analises['top_10_idh'], 'top_10_municipios_idh.csv')
        
        # 6. IDH por estado
        self.exporter.to_csv(analises['idh_por_estado'], 'idh_por_estado.csv')
        
        # 7. IDH por região
        self.exporter.to_csv(analises['idh_por_regiao'], 'idh_por_regiao.csv')
        
        # 8. Evolução temporal
        self.exporter.to_csv(analises['evolucao_temporal'], 'evolucao_temporal_idh.csv')
        
        # 9. Análise dos componentes
        df_componentes = pd.DataFrame([analises['componentes_idh']])
        self.exporter.to_csv(df_componentes, 'componentes_idh.csv')
        
        # 10. Relatório completo
        self._gerar_relatorio_completo(resultados)
    
    def _gerar_relatorio_completo(self, resultados):
        """Gera relatório detalhado com todos os insights"""
        analises = resultados['analises']
        
        with open(os.path.join(self.output_path, 'relatorio_analise_idh.txt'), 'w', encoding='utf-8') as f:
            f.write("RELATÓRIO COMPLETO - ANÁLISE IDH BRASIL\n")
            f.write("=" * 60 + "\n\n")
            
            # Informações básicas
            stats = analises['estatisticas_gerais']
            f.write("INFORMAÇÕES GERAIS:\n")
            f.write(f"- Período analisado: {min(stats['anos_analisados'])} a {max(stats['anos_analisados'])}\n")
            f.write(f"- Ano mais recente: {analises['ano_mais_recente']}\n")
            f.write(f"- Total de registros: {stats['total_municipios']}\n")
            f.write(f"- IDH Médio Nacional: {stats['media_idh']:.4f}\n")
            f.write(f"- IDH Máximo: {stats['max_idh']:.4f}\n")
            f.write(f"- IDH Mínimo: {stats['min_idh']:.4f}\n\n")
            
            # Categorias
            f.write("DISTRIBUIÇÃO POR CATEGORIAS DE IDH:\n")
            for categoria, quantidade in analises['categorias_idh'].items():
                percentual = (quantidade / stats['total_municipios']) * 100
                f.write(f"- {categoria}: {quantidade} municípios ({percentual:.1f}%)\n")
            f.write("\n")
            
            # Top 10
            f.write("TOP 10 MUNICÍPIOS COM MELHOR IDH:\n")
            for idx, row in analises['top_10_idh'].iterrows():
                f.write(f"- {row['município']} ({row['uf']}): {row['idhm']:.4f}\n")
            f.write("\n")
            
            # Análise por região
            f.write("IDH MÉDIO POR REGIÃO:\n")
            for regiao, dados in analises['idh_por_regiao'].iterrows():
                f.write(f"- {regiao}: {dados['mean']:.4f}\n")
            f.write("\n")
            
            # Componentes do IDH
            comp = analises['componentes_idh']
            f.write("COMPONENTES DO IDH (médias):\n")
            f.write(f"- Educação (idhm_e): {comp['idhm_e_media']:.4f}\n")
            f.write(f"- Longevidade (idhm_l): {comp['idhm_l_media']:.4f}\n")
            f.write(f"- Renda (idhm_r): {comp['idhm_r_media']:.4f}\n")
            f.write(f"- Correlação Educação-Renda: {comp['correlacao_educacao_renda']:.4f}\n")
            f.write(f"- Correlação Longevidade-Renda: {comp['correlacao_longevidade_renda']:.4f}\n")