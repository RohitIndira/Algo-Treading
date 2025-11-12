package main

import (
	"log"

	"github.com/RohitIndira/Algo-Treading/services/risk-management/config"
	"github.com/RohitIndira/Algo-Treading/services/risk-management/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/risk-management/internal/server"
)

func main() {
	cfg := config.LoadConfig()
	redisAddr := cfg.RedisHost + ":" + cfg.RedisPort
	redisRepo := repository.NewRedisRepository(redisAddr)

	s := server.NewRiskManagementServer(redisRepo)
	if err := s.Start(cfg.GRPCPort); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
