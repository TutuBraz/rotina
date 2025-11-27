import { test, expect } from '@playwright/test';
import AdmZip from 'adm-zip';
import nodemailer from 'nodemailer';
import fs from 'fs';

test('Bradesco custódia', async ({ page }) => {
  test.setTimeout(200000);
  let nomeNovo: string | undefined;
  let nomeNovoP: string | undefined;

  //Data
  const hoje = new Date();
  const ontem = new Date(hoje);
  ontem.setDate(hoje.getDate() - 2);
  const diaOntem = String(ontem.getDate()).padStart(2, '0');
  const mesOntem = String(ontem.getMonth() + 1).padStart(2, '0');
  const anoOntem = String(ontem.getFullYear());

  //Login
  await page.goto('https://bc2.custodia.bradesco/calilogin/login.jsf');
  await page.locator('#param1').click();
  await page.locator('#param1').fill('WGS83837');
  await page.locator('#param1').press('Enter');
  await page.locator('#param2').click();
  await page.locator('#param2').fill('Getec@1473');
  await page.locator('#param2').press('Enter');
  //Acessar Página XML Anbima
  await page.getByRole('link', { name: 'Custódia e Controladoria de' }).click();
  await page.locator('iframe[name="paginaCentral"]').contentFrame().getByRole('heading', { name: 'Relatórios' }).click();
  await page.locator('iframe[name="paginaCentral"]').contentFrame().getByRole('link', { name: 'XML Anbima' }).click();
  const frameLocator = page.frameLocator('iframe[name="paginaCentral"]');
  await frameLocator.locator('button:has(i.search)').click();

  //--------------------------FUNDOS-------------------
  //Fundos Ceres
  const modalFrame = page.frameLocator('iframe[name="modal_infra_estrutura"]');
  await modalFrame.locator('a#form\\:_id39').click();
  await modalFrame.getByText('27534', { exact: true }).waitFor({ timeout: 10000 });
  await modalFrame.getByText('27534', { exact: true }).click();
  const frame = page.frameLocator('iframe[name="paginaCentral"]');
  await frame.locator('input#dados\\:_id62Dia').fill(diaOntem);
  await frame.locator('input#dados\\:_id62Mes').fill(mesOntem);
  await frame.locator('input#dados\\:_id62Ano').fill(anoOntem);
  await frame.locator('a#dados\\:_id64').click();
  //Capturar número de protocolo
  const numeroProtocolo = frame.locator(
    'text=Número de Protocolo: >> xpath=following-sibling::span'
  );
  await numeroProtocolo.waitFor();
  const textoCompleto = await numeroProtocolo.textContent();
  const protocolo = textoCompleto?.match(/\d+/)?.[0] ?? '';
  console.log('Número de Protocolo Fundos:', protocolo);
  await expect(protocolo).toMatch(/^\d+$/);
  //Baixar arquivo Fundos
  await page.getByRole('link', { name: 'Custódia e Controladoria de' }).click();
  await page.locator('iframe[name="paginaCentral"]').contentFrame().getByRole('heading', { name: 'Relatórios' }).click();
  await page.locator('iframe[name="paginaCentral"]').contentFrame().getByText('Extração de Arquivos (Book /').click();
  await page.locator('iframe[name="paginaCentral"]').contentFrame().getByRole('link', { name: 'Protocolo de Book e Grupo de' }).click();
  await frame.locator('#dados\\:_id41').fill(protocolo);
  await page.waitForTimeout(40000);
  await frame.locator('#dados\\:_id42').click();
  await page.waitForTimeout(10000);
  await frame.getByText(protocolo, { exact: true }).waitFor({ timeout: 10000 });
  
  try{
    const [download] = await Promise.all([
      page.waitForEvent('download',{timeout:10000}),
      frame.getByText(protocolo, { exact: true }).click(),
    ]);
    const pathTemp = await download.path();
    const nomeOriginal = download.suggestedFilename();
    nomeNovo = `${nomeOriginal.replace('.zip', '')}_${diaOntem}-${mesOntem}-${anoOntem}_FUNDOS.zip`;
    await download.saveAs(`downloads/${nomeNovo}`);
    console.log(`✅ Arquivo salvo como: downloads/${nomeNovo}`)}
  catch(err){
    console.warn("⚠️ Nenhum download foi detectado. Continuando execução...")
  }

//--------------------------PLANOS-------------------
  //Acessar Página XML Anbima
  await page.getByRole('link', { name: 'Custódia e Controladoria de' }).click();
  await page.locator('iframe[name="paginaCentral"]').contentFrame().getByRole('heading', { name: 'Relatórios' }).click();
  await page.locator('iframe[name="paginaCentral"]').contentFrame().getByRole('link', { name: 'XML Anbima' }).click();
  await frameLocator.locator('button:has(i.search)').click();

  //Planos Ceres
  await modalFrame.locator('a#form\\:_id39').click();
  await modalFrame.getByText('27533', { exact: true }).waitFor({ timeout: 10000 });
  await modalFrame.getByText('27533', { exact: true }).click();
  await frame.locator('input#dados\\:_id62Dia').fill(diaOntem);
  await frame.locator('input#dados\\:_id62Mes').fill(mesOntem);
  await frame.locator('input#dados\\:_id62Ano').fill(anoOntem);
  await frame.locator('a#dados\\:_id64').click();

  //Capturar número de protocolo
  const numeroProtocoloP = frame.locator(
    'text=Número de Protocolo: >> xpath=following-sibling::span'
  );
  await numeroProtocoloP.waitFor();
  const textoCompletoP = await numeroProtocoloP.textContent();
  const protocoloP = textoCompletoP?.match(/\d+/)?.[0] ?? '';
  console.log('Número de Protocolo Planos:', protocoloP);
  await expect(protocolo).toMatch(/^\d+$/);

  //Baixar arquivo Planos
  await page.getByRole('link', { name: 'Custódia e Controladoria de' }).click();
  await page.locator('iframe[name="paginaCentral"]').contentFrame().getByRole('heading', { name: 'Relatórios' }).click();
  await page.locator('iframe[name="paginaCentral"]').contentFrame().getByText('Extração de Arquivos (Book /').click();
  await page.locator('iframe[name="paginaCentral"]').contentFrame().getByRole('link', { name: 'Protocolo de Book e Grupo de' }).click();
  await frame.locator('#dados\\:_id41').fill(protocoloP);
  await page.waitForTimeout(40000);
  await frame.locator('#dados\\:_id42').click();
  await page.waitForTimeout(10000);
  await frame.getByText(protocoloP, { exact: true }).waitFor({ timeout: 10000 });

  try{
    const [downloadP] = await Promise.all([
      page.waitForEvent('download'),
      frame.getByText(protocoloP, { exact: true }).click(),
    ]);
    const pathTempP = await downloadP.path();
    const nomeOriginalP = downloadP.suggestedFilename();
    nomeNovoP = `${nomeOriginalP.replace('.zip', '')}_${diaOntem}-${mesOntem}-${anoOntem}_PLANOS.zip`;
    await downloadP.saveAs(`downloads/${nomeNovoP}`);
    console.log(`✅ Arquivo salvo como: downloads/${nomeNovoP}`);
  }catch(err){
    console.warn("⚠️ Nenhum download foi detectado. Continuando execução...")
  }


  //Contagem de arquivos
  let qtdFundos = 0;
  let qtdPlanos = 0;

  const arquivosZip = [
    { path: `downloads/${nomeNovo}`, tipo: 'FUNDOS' },
    { path: `downloads/${nomeNovoP}`, tipo: 'PLANOS' },
  ];

  for (const { path: zipPath, tipo } of arquivosZip) {
    if (fs.existsSync(zipPath)) {
      const zip = new AdmZip(zipPath);
      const arquivos = zip.getEntries().filter(entry => !entry.isDirectory);
      console.log(`📦 O arquivo ${zipPath} contém ${arquivos.length} arquivos.`);

      if (tipo === 'FUNDOS') qtdFundos = arquivos.length;
      if (tipo === 'PLANOS') qtdPlanos = arquivos.length;
    } else {
      console.log(`⚠️ Arquivo não encontrado: ${zipPath}`);
    }
  }

  console.log(`🔹 FUNDOS: ${qtdFundos} arquivos`);
  console.log(`🔹 PLANOS: ${qtdPlanos} arquivos`);

  //Envio de arquivos
    const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: 'rpa@ceres.org.br',
      pass: 'vilu lqpn zhst bhyl'
    }
  });

  // Anexos (se quiser mandar os zips gerados pelo Playwright)
  const anexos = [
    { filename: nomeNovo, path: `downloads/${nomeNovo}` },
    { filename: nomeNovoP, path: `downloads/${nomeNovoP}` }
  ];

  // Corpo do e-mail
  const mailOptions = {
    from: '"Automação Ceres" <rpa@ceres.org.br>',
    to: 'caio.castro@ceres.org.br, pedro.silva@ceres.org.br, adoniel.carvalho@gmail.com',
    subject: `Arquivos Bradesco Custódia - ${diaOntem}/${mesOntem}/${anoOntem}`,
    text: `Segue em anexo os arquivos extraídos pela automação 
  Resumo dos Arquivos: 
      - Fundos: ${qtdFundos} arquivos extraídos
      - Planos: ${qtdPlanos} arquivos extraídos`,
    attachments: anexos
  };

  // Enviar o e-mail
  await transporter.sendMail(mailOptions);
  console.log('✅ E-mail enviado com sucesso!');

});
