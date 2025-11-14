import sys
import os

sys.path.append(os.path.dirname(__file__))

from scripts.idh_pipeline import IDHDataPipeline

def main():
    print("=" * 60)
    print("📊 PIPELINE DE ANÁLISE DE IDH - DATASET KAGGLE HDI BRAZIL")
    print("=" * 60)
    
    pipeline = IDHDataPipeline()
    
    print("🔍 Procurando dataset do Kaggle HDI Brazil...")
    resultado = pipeline.run_kaggle_idh_analysis()
    
    if resultado is not None:
        print("\n🎉 ANÁLISE CONCLUÍDA COM SUCESSO!")
        print("📍 Resultados disponíveis em: src/data/output/")
        print("\n📁 Arquivos gerados:")
        
        output_files = os.listdir(pipeline.output_path)
        for file in output_files:
            print(f"   📄 {file}")
            
    else:
        print("\n❌ Análise não pôde ser concluída.")
        print("💡 Verifique se o dataset do Kaggle está na pasta src/data/raw/")

if __name__ == "__main__":
    main()