package tg_bot_gateway

import "fmt"

// referralScreenText формирует экран реферальной программы.
func referralScreenText(stats *referralStatsView, botUsername string) string {
	link := referralLink(botUsername, stats.Code)

	remainder := 0
	if stats.UsersPerMonth > 0 {
		remainder = stats.UsersPerMonth - (stats.InvitedTotal % stats.UsersPerMonth)
		if remainder == stats.UsersPerMonth {
			remainder = 0
		}
	}

	var sb []byte
	add := func(s string) { sb = append(sb, s...) }

	add("🎁 *Реферальная программа*\n\n")
	add(fmt.Sprintf("За каждые *%d* друзей, оформивших платную подписку, вы получаете *1 месяц бесплатно*.\n\n", stats.UsersPerMonth))
	add(fmt.Sprintf("👥 Приглашено (с оплатой): *%d*\n", stats.InvitedTotal))
	add(fmt.Sprintf("🎟 Доступно бесплатных месяцев: *%d*\n", stats.AvailableMonths))
	if stats.AvailableMonths > 0 {
		add("\n✅ Есть доступные месяцы! Нажмите *«Получить бесплатные месяцы»*.\n")
	} else if remainder > 0 {
		add(fmt.Sprintf("\nЕщё *%d* друзей — и получите следующий бесплатный месяц.\n", remainder))
	}
	add("\n🔗 Ваша ссылка для приглашений:\n")
	add("`" + link + "`\n")
	add("\nПоделитесь ей с друзьями — как только они оформят платную подписку, приглашение засчитается.")

	return string(sb)
}
